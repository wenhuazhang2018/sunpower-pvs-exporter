import asyncio
import aiohttp
import time
import logging
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from pypvs.pvs import PVS
from pypvs.updaters.meter import PVSProductionMetersUpdater
from pypvs.updaters.production_inverters import PVSProductionInvertersUpdater
from pypvs.models.pvs import PVSData
from pypvs.models.common import CommonProperties
from pypvs.const import SupportedFeatures
from pypvs.exceptions import ENDPOINT_PROBE_EXCEPTIONS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class SunPowerSnapshotCollector:
    def __init__(self, host, user="ssm_owner"):
        self.host = host
        self.user = user
    def collect(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._collect_async())

    async def _collect_async(self):
        async with aiohttp.ClientSession() as session:
            pvs = PVS(session=session, host=self.host, user=self.user)
            try:
                await pvs.discover()
                await pvs.setup(auth_password=pvs.serial_number[-5:])
            except ENDPOINT_PROBE_EXCEPTIONS:
                logging.error("Unauthorized access. Retrying login failed.")
                return []
            except: 
                loggin.error("error connect to PVS.")
                return[]

            meter_updater = PVSProductionMetersUpdater(pvs.getVarserverVar, pvs.getVarserverVars, CommonProperties())
            try:
                inverter_updater = PVSProductionInvertersUpdater(pvs.getVarserverVar, pvs.getVarserverVars, CommonProperties())
                await meter_updater.probe(SupportedFeatures(0))
                await inverter_updater.probe(SupportedFeatures(0))
            except PVSError as e:
                logging.warning(f"Inverter likely offline or sleeping: {e}")
            except Exception as e:
                logging.error(f"Unexpected inverter error: {e}")
            pvs_data = PVSData()
            await meter_updater.update(pvs_data)
            try:
                await inverter_updater.update(pvs_data)
            except:
                print ("inverter sleeping?")

            metrics = []
            meter_labels = ["ct_rated_current",
                           "description",
                           "device_id",
                           "mode", "model",
                           "port",
                           "software_version",
                           "type",
                          ]
            inverter_labels =[           "description",
                                         "device_id",
                                         "model",
                                         "module_id",
                                         "port",
                                         "software_version",
                                         "type",
                                        ]


            ct_metric = GaugeMetricFamily(
                "sunpower_pvs_power_meter_ct_rated_current_amperes",
                "CT Rated Current",
                labels=[
                    "ct_rated_current", "description", "device_id",
                    "mode", "model", "port", "software_version", "type"
                ]
            )

            frequency = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_ac_frequency",
                documentation="AC Frequency",
                labels=[
                    "ct_rated_current", "description", "device_id",
                    "mode", "model", "port", "software_version", "type"
                ],
                unit="hertz",
            )
            energy_total = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_net_energy",
                documentation="Total Net Energy",
                labels=[
                    "ct_rated_current", "description", "device_id",
                    "mode", "model", "port", "software_version", "type"
                ],
                unit="watt_hours",
            )
            real_power = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_average_real_power",
                documentation="Average real power",
                labels=[
                    "ct_rated_current", "description", "device_id",
                    "mode", "model", "port", "software_version", "type"
                ],
                unit="watts",
            )
            reactive_power = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_average_reactive_power",
                documentation="Average reactive power",
                labels=[
                    "ct_rated_current", "description", "device_id",
                    "mode", "model", "port", "software_version", "type"
                ],
                unit="volt_amperes_reactive",
            )
            apparent_power = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_average_apparent_power",
                documentation="Average reactive power",
                labels=meter_labels,
                unit="volt_amperes_reactive"
            )
            power_factor = GaugeMetricFamily(
                name="sunpower_pvs_power_meter_power_factor_real_power_per_apparent_power",
                documentation="Power Factor (Real Power / Apparent Power) ratio",
                labels=meter_labels,
            )


   
            
            for meter in pvs_data.meters.values():
                serial = meter.serial_number
                mode = "production" if serial.endswith("p") else "consumption"
                ct_value = float(meter.ct_scale_factor)
                fq_value = float(meter.freq_hz)
            
                ct_metric.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], ct_value)
                frequency.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], fq_value)
                energy_total.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], float(meter.net_lte_kwh)*1000)
                real_power.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], (float(meter.power_3ph_kw))*1000)

                reactive_power.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], float(meter.q3phsum_kvar))
                apparent_power.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], float(meter.s3phsum_kva))
                power_factor.add_metric([
                    f"{int(ct_value)}",
                    f"Power Meter {serial}",
                    serial,
                    mode,
                    meter.model,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    f"PVS6-METER-{mode[0].upper()}"  # type: PVS5-METER-P or -C
                ], (float(meter.power_3ph_kw))/float(meter.s3phsum_kva))

        
            metrics.append(ct_metric)
            metrics.append(frequency)
            metrics.append(energy_total)
            metrics.append(real_power)
            metrics.append(reactive_power)
            metrics.append(apparent_power)
            metrics.append(power_factor)

            labels = inverter_labels
            ac_current = GaugeMetricFamily(
                name="sunpower_pvs_inverter_ac_current",
                documentation="AC Current",
                labels=labels,
                unit="amperes",
            )
            ac_power = GaugeMetricFamily(
                name="sunpower_pvs_inverter_ac_power",
                documentation="AC Power",
                labels=labels,
                unit="watts",
            )
            ac_voltage = GaugeMetricFamily(
                name="sunpower_pvs_inverter_ac_voltage",
                documentation="AC Voltage",
                labels=labels,
                unit="volts",
            )
            dc_current = GaugeMetricFamily(
                name="sunpower_pvs_inverter_dc_current",
                documentation="DC Current",
                labels=labels,
                unit="amperes",
            )
            dc_power = GaugeMetricFamily(
                name="sunpower_pvs_inverter_dc_power",
                documentation="DC Power",
                labels=labels,
                unit="watts",
            )
            dc_voltage = GaugeMetricFamily(
                name="sunpower_pvs_inverter_dc_voltage",
                documentation="DC Voltage",
                labels=labels,
                unit="volts",
            )
            frequency = GaugeMetricFamily(
                name="sunpower_pvs_inverter_operating_frequency",
                documentation="Operating Frequency (hertz)",
                labels=labels,
                unit="hertz",
            )
            energy = GaugeMetricFamily(
                name="sunpower_pvs_inverter_energy_total_watt_hours",
                documentation="Total Energy",
                labels=labels,
                unit="watt_hours",
            )
            heatsink_temp = GaugeMetricFamily(
                name="sunpower_pvs_inverter_heatsink_temperature",
                documentation="Heatsink Temperature",
                labels=labels,
                unit="celsius",
            )
            
            for inverter in pvs_data.inverters.values():

                serial = inverter.serial_number
                model = inverter.model

                '''
                inverter_metric.add_metric([
                    inverter.serial_number,
                    inverter.model,                    
                    f"{float(inverter.last_report_kw):.3f}",
                    f"{float(inverter.last_report_voltage_v):.3f}",
                    f"{float(inverter.last_report_current_a):.3f}",                    
                    f"{float(inverter.last_report_frequency_hz):.3f}",
                    f"{float(inverter.lte_kwh):.3f}",
                    f"{float(inverter.last_report_temperature_c):.1f}",
                    str(inverter.last_report_date)
                ], 1)
                '''
                
                ac_power.add_metric([
                    f"Inverter {serial}",
                    serial,
                    model,
                    serial,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    "ENPHASE"  # type: PVS5-METER-P or -C
                ],float(inverter.last_report_kw)*1000)
                heatsink_temp.add_metric([
                    f"Inverter {serial}",
                    serial,
                    model,
                    serial,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    "ENPHASE"  # type: PVS5-METER-P or -C
                ],float(inverter.last_report_temperature_c))
                energy.add_metric([
                    f"Inverter {serial}",
                    serial,
                    model,
                    serial,
                    "",               # port not exposed
                    "unknown",        # software_version not exposed
                    "ENPHASE" # type: PVS5-METER-P or -C
                ],float(inverter.lte_kwh))

            metrics.append(ac_power)
            metrics.append(heatsink_temp)
            metrics.append(energy)
            

            return metrics

if __name__ == "__main__":
    host = "192.168.1.222"  # Replace with your PVS IP
    port = 8000             # Prometheus will scrape http://localhost:8000/metrics
    REGISTRY.register(SunPowerSnapshotCollector(host))
    start_http_server(port)
    logging.info(f"Prometheus snapshot exporter running on port {port}...")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Exporter shutting down.")
