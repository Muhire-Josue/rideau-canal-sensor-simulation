import asyncio
import json
import os
import random
from datetime import datetime, timezone

from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from dotenv import load_dotenv

# Load .env file
load_dotenv()

SEND_INTERVAL_SECONDS = 10  # send every 10 seconds

# Device configs: env var name + human location name + deviceId
DEVICES_CONFIG = [
    {
        "env_var": "DEVICE_DOWS_LAKE",
        "location": "Dows Lake",
        "device_id": "dows-lake-device",
    },
    {
        "env_var": "DEVICE_FIFTH_AVENUE",
        "location": "Fifth Avenue",
        "device_id": "fifth-avenue-device",
    },
    {
        "env_var": "DEVICE_NAC",
        "location": "NAC",
        "device_id": "nac-device",
    },
]


def generate_sensor_reading(device_id: str, location: str) -> dict:
    """
    Generate one fake sensor reading for a given device/location.
    Values are chosen to be realistic-ish and give Safe/Caution/Unsafe mixes.
    """
    # Base ice thickness in cm
    ice_thickness = random.uniform(22.0, 38.0)  # some will be unsafe, some safe
    # Surface temperature in °C
    surface_temp = random.uniform(-12.0, 2.0)
    # Snow accumulation in cm
    snow_accum = random.uniform(0.0, 10.0)
    # External air temperature in °C
    external_temp = surface_temp + random.uniform(-3.0, 3.0)

    return {
        "deviceId": device_id,
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "iceThicknessCm": round(ice_thickness, 2),
        "surfaceTemperatureC": round(surface_temp, 2),
        "snowAccumulationCm": round(snow_accum, 2),
        "externalTemperatureC": round(external_temp, 2),
    }


async def send_device_data(device_config: dict):
    """
    Connect a single virtual device to IoT Hub and send telemetry in a loop.
    """
    conn_str = os.getenv(device_config["env_var"])
    if not conn_str:
        print(f"[ERROR] Missing environment variable: {device_config['env_var']}")
        return

    print(
        f"[INFO] Connecting device '{device_config['device_id']}' "
        f"({device_config['location']}) to IoT Hub..."
    )
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    await device_client.connect()
    print(
        f"[INFO] Device '{device_config['device_id']}' connected. "
        f"Sending telemetry every {SEND_INTERVAL_SECONDS} seconds."
    )

    try:
        while True:
            payload = generate_sensor_reading(
                device_config["device_id"], device_config["location"]
            )
            msg = Message(json.dumps(payload))
            msg.content_encoding = "utf-8"
            msg.content_type = "application/json"

            await device_client.send_message(msg)
            print(f"[{device_config['device_id']}] Sent: {payload}")

            await asyncio.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"\n[INFO] Stopping device '{device_config['device_id']}'...")
    finally:
        await device_client.disconnect()
        print(f"[INFO] Device '{device_config['device_id']}' disconnected.")


async def main():
    # Launch a sending loop for each device in parallel
    tasks = []
    for cfg in DEVICES_CONFIG:
        tasks.append(asyncio.create_task(send_device_data(cfg)))

    # Run until cancelled (Ctrl+C)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Sensor simulator stopped by user.")
