import asyncio
import httpx
import pandas as pd

from weather_info import weather_country, weather_start_date_end_date


async def fetch_weather_data(file_name: str, params_data: dict):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": "11.992085844591886",
        "longitude": "105.46402914861625",
        "start_date": "2024-01-01",
        "end_date": "2024-10-26",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                 "apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,"
                 "wind_speed_10m_max,wind_gusts_10m_max",
        **params_data
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        data = response.json()

        # Extract relevant daily data
        daily_data = data.get("daily", {})
        output_data = {
            "time": daily_data.get("time", []),
            "weather_code": daily_data.get("weather_code", []),
            "temperature_2m_max": daily_data.get("temperature_2m_max", []),
            "temperature_2m_min": daily_data.get("temperature_2m_min", []),
            "temperature_2m_mean": daily_data.get("temperature_2m_mean", []),
            "apparent_temperature_max": daily_data.get("apparent_temperature_max", []),
            "apparent_temperature_min": daily_data.get("apparent_temperature_min", []),
            "apparent_temperature_mean": daily_data.get("apparent_temperature_mean", []),
            "wind_speed_10m_max": daily_data.get("wind_speed_10m_max", []),
            "wind_gusts_10m_max": daily_data.get("wind_gusts_10m_max", [])
        }

        # Convert to DataFrame
        df = pd.DataFrame(output_data)

        # Save to CSV
        df.to_csv(f"{file_name}.csv", index=False,)
        print(f"Data saved to {file_name}.csv")


for country, latitude_longitude in weather_country.items():
    # Run the async function
    for start_date_end_date in weather_start_date_end_date:
        file_name = f"{country} {start_date_end_date[0]} - {start_date_end_date[-1]} - Open Meteo.csv"
        params_data = {
            "latitude": f"{latitude_longitude[0]}",
            "longitude": f"{latitude_longitude[-1]}",
            "start_date": f"{start_date_end_date[0]}",
            "end_date": f"{start_date_end_date[-1]}"
        }
        asyncio.run(fetch_weather_data(file_name, params_data))
