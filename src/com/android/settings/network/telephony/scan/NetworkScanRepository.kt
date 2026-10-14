/*
 * Copyright (C) 2024 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.settings.network.telephony.scan

import android.content.Context
import android.telephony.AccessNetworkConstants.AccessNetworkType
import android.telephony.CellInfo
import android.telephony.CarrierConfigManager
import android.telephony.CellInfoGsm
import android.telephony.CellInfoLte
import android.telephony.CellInfoWcdma
import android.telephony.NetworkScan
import android.telephony.NetworkScanRequest
import android.telephony.PhoneCapability
import android.telephony.RadioAccessSpecifier
import android.telephony.TelephonyManager
import android.telephony.TelephonyScanManager
import android.util.Log
import androidx.lifecycle.LifecycleOwner
import com.android.internal.telephony.CellNetworkScanResult
import com.android.settings.R
import com.android.settings.network.telephony.CellInfoUtil.getNetworkTitle
import com.android.settings.network.telephony.CellInfoUtil.getOperatorNumeric
import com.android.settings.network.telephony.CellInfoUtil.toCellInfo
import com.android.settings.network.telephony.telephonyManager
import com.android.settingslib.spa.framework.util.collectLatestWithLifecycle
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onEach

class NetworkScanRepository(private val context: Context, private val subId: Int) {
    enum class NetworkScanState {
        ACTIVE, COMPLETE, ERROR
    }

    data class NetworkScanResult(
        val state: NetworkScanState,
        val cellInfos: List<CellInfo>,
    )

    private val telephonyManager = context.telephonyManager(subId)

    /** TODO: Move this to UI layer, when UI layer migrated to Kotlin. */
    fun launchNetworkScan(lifecycleOwner: LifecycleOwner, onResult: (NetworkScanResult) -> Unit) =
        networkScanFlow().collectLatestWithLifecycle(lifecycleOwner, action = onResult)

    data class CellInfoScanKey(
        val title: String?,
        val className: String,
        val isRegistered: Boolean,
    ) {
        constructor(cellInfo: CellInfo) : this(
            title = cellInfo.cellIdentity.getNetworkTitle(),
            className = cellInfo.javaClass.name,
            isRegistered = cellInfo.isRegistered,
        )
    }

    fun networkScanFlow(): Flow<NetworkScanResult> = callbackFlow {
        var state = NetworkScanState.ACTIVE
        var cellInfos: List<CellInfo> = emptyList()
        val active = AtomicBoolean(true)
        // Keep callbacks serialized and release the worker when this collection ends.
        val executor = Executors.newSingleThreadExecutor()

        fun sendResult() {
            if (active.get()) trySend(NetworkScanResult(state, cellInfos))
        }

        fun finishWithVisibleCells() {
            if (!active.get()) return
            val fallbackCells = getFallbackCellInfos()
            if (!active.get()) return
            if (fallbackCells.isNotEmpty() && cellInfos.isEmpty()) {
                cellInfos = fallbackCells.distinctBy { CellInfoScanKey(it) }
                state = NetworkScanState.COMPLETE
            } else {
                state = NetworkScanState.ERROR
            }
            sendResult()
            active.set(false)
            close()
        }

        val callback = object : TelephonyScanManager.NetworkScanCallback() {
            override fun onResults(results: List<CellInfo>) {
                if (!active.get()) return
                cellInfos = results.distinctBy { CellInfoScanKey(it) }
                sendResult()
            }

            override fun onComplete() {
                if (!active.get()) return
                state = NetworkScanState.COMPLETE
                sendResult()
                // Some modems deliver their final onResults() after onComplete().
            }

            override fun onError(error: Int) {
                if (!active.get()) return
                if (error == NetworkScan.ERROR_UNSUPPORTED) {
                    Log.d(TAG, "network scan unsupported, falling back to legacy query")
                    queryAvailableNetworks()
                } else {
                    Log.w(TAG, "network scan onError: $error, falling back to visible cells")
                    finishWithVisibleCells()
                }
            }

            private fun queryAvailableNetworks() {
                val result = try {
                    telephonyManager.getAvailableNetworks()
                } catch (e: RuntimeException) {
                    Log.w(TAG, "legacy network query failed", e)
                    finishWithVisibleCells()
                    return
                }
                // The blocking Binder query has no cancellation API. Drop its response
                // when the lifecycle has already ended this collection.
                if (!active.get()) return
                if (result?.status != CellNetworkScanResult.STATUS_SUCCESS) {
                    finishWithVisibleCells()
                    return
                }
                cellInfos = result.operators.orEmpty()
                    .mapNotNull { it.toCellInfo() }
                    .groupBy { it.cellIdentity.getOperatorNumeric() }
                    .values
                    .map { infos ->
                        infos.maxWith(compareBy<CellInfo>(
                            { it.isRegistered },
                            { LEGACY_RAT_ORDER.indexOf(it.javaClass) },
                        ))
                    }
                state = NetworkScanState.COMPLETE
                sendResult()
                active.set(false)
                close()
            }
        }

        val networkScan = try {
            telephonyManager.requestNetworkScan(createNetworkScan(), executor, callback)
                .also { if (it == null) finishWithVisibleCells() }
        } catch (e: RuntimeException) {
            Log.e(TAG, "requestNetworkScan failed to start", e)
            finishWithVisibleCells()
            null
        }

        awaitClose {
            active.set(false)
            try {
                networkScan?.stopScan()
            } catch (e: RuntimeException) {
                Log.w(TAG, "network scan stopScan failed", e)
            } finally {
                executor.shutdownNow()
            }
            Log.d(TAG, "network scan stopped")
        }
    }.conflate().onEach { Log.d(TAG, "networkScanFlow: $it") }.flowOn(Dispatchers.Default)

    private fun getFallbackCellInfos(): List<CellInfo> = runCatching {
        telephonyManager.allCellInfo?.filter {
            it.cellIdentity.getNetworkTitle() != null
        } ?: emptyList()
    }.getOrDefault(emptyList())

    /** Create network scan for allowed network types. */
    private fun createNetworkScan(): NetworkScanRequest {
        val allowedNetworkTypes = getAllowedNetworkTypes()
        Log.d(TAG, "createNetworkScan: allowedNetworkTypes = $allowedNetworkTypes")
        val radioAccessSpecifiers = allowedNetworkTypes
            .map { RadioAccessSpecifier(it, null, null) }
            .toTypedArray()
        val isIncremental = runCatching {
            val carrierConfigManager = context.getSystemService(CarrierConfigManager::class.java)
            carrierConfigManager?.getConfigForSubId(subId)
                ?.getBoolean("incremental_network_scan_supported_bool", false)
        }.getOrNull() ?: false
        return NetworkScanRequest(
            NetworkScanRequest.SCAN_TYPE_ONE_SHOT,
            radioAccessSpecifiers,
            NetworkScanRequest.MIN_SEARCH_PERIODICITY_SEC, // one shot, not used
            context.resources.getInteger(R.integer.config_network_scan_helper_max_search_time_sec),
            isIncremental,
            INCREMENTAL_RESULTS_PERIODICITY_SEC,
            null,
        )
    }

    private fun getAllowedNetworkTypes(): List<Int> {
        val networkTypeBitmap3gpp: Long =
            telephonyManager.getAllowedNetworkTypesBitmask() and
                TelephonyManager.NETWORK_STANDARDS_FAMILY_BITMASK_3GPP
        return buildList {
            // If the allowed network types are unknown or if they are of the right class, scan for
            // them; otherwise, skip them to save scan time and prevent users from being shown
            // networks that they can't connect to.
            if (networkTypeBitmap3gpp == 0L
                || networkTypeBitmap3gpp and TelephonyManager.NETWORK_CLASS_BITMASK_2G != 0L
            ) {
                add(AccessNetworkType.GERAN)
            }
            if (networkTypeBitmap3gpp == 0L
                || networkTypeBitmap3gpp and TelephonyManager.NETWORK_CLASS_BITMASK_3G != 0L
            ) {
                add(AccessNetworkType.UTRAN)
            }
            if (networkTypeBitmap3gpp == 0L
                || networkTypeBitmap3gpp and TelephonyManager.NETWORK_CLASS_BITMASK_4G != 0L
            ) {
                add(AccessNetworkType.EUTRAN)
            }
            // If a device supports 5G stand-alone then the code below should be re-enabled; however
            // a device supporting only non-standalone mode cannot perform PLMN selection and camp
            // on a 5G network, which means that it shouldn't scan for 5G at the expense of battery
            // as part of the manual network selection process.
            //
            if (networkTypeBitmap3gpp == 0L
                || (networkTypeBitmap3gpp and TelephonyManager.NETWORK_CLASS_BITMASK_5G != 0L &&
                    hasNrSaCapability())
            ) {
                add(AccessNetworkType.NGRAN)
                Log.d(TAG, "radioAccessSpecifiers add NGRAN.")
            }
        }
    }

    private fun hasNrSaCapability(): Boolean {
        val phoneCapability = telephonyManager.getPhoneCapability()
        return PhoneCapability.DEVICE_NR_CAPABILITY_SA in phoneCapability.deviceNrCapabilities
    }

    companion object {
        private const val TAG = "NetworkScanRepository"

        private const val INCREMENTAL_RESULTS_PERIODICITY_SEC = 3

        private val LEGACY_RAT_ORDER = listOf(
            CellInfoGsm::class.java,
            CellInfoWcdma::class.java,
            CellInfoLte::class.java,
        )
    }
}
