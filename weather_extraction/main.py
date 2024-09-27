import json
from datetime import datetime
import time
import pandas as pd 
import requests
import pytz

utc = pytz.UTC

currentTime = datetime.time(datetime.now())

#we want mumbai's weather data from API
city_name = 'Mumbai'

#base URL for API call
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='

# accessing the API key from location

with open("/Weather_Data_Extraction_From_API/API_KEY/credentials.txt", 'r') as f:
    api_key = f.read()

# Creating url to fetch data from API

full_url = base_url + city_name + "&APPID=" + api_key

#created empty list to store the data

transformed_data_list = []


#converting the tempreture in farenheit 

def kelvinTofarenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return round(temp_in_fahrenheit,2)

#calling function to get data from requests method in json fromate

def etl_weather_data(full_url):

    r = requests.get(full_url)
    data = r.json()

    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temp_farenheit = kelvinTofarenheit(data["main"]["temp"])
    feels_like_farenheit = kelvinTofarenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvinTofarenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvinTofarenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data["timezone"], tz=utc)
    sunrise_time = datetime.fromtimestamp(data["sys"]["sunrise"], tz = utc)
    sunset_time = datetime.fromtimestamp(data["sys"]["sunset"], tz = utc)

    transformed_data = {
        "City":city,
        "Description":weather_description,
        "Tempreture (F)":temp_farenheit,
        "Feels Like (F)": feels_like_farenheit,
        "Minimun Temp (F)":min_temp_farenheit,
        "Maximum Temp (F)": max_temp_farenheit,
        "Pressure": pressure,
        "Humidty": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)":sunrise_time,
        "Sunset (Local Time)": sunset_time  
    }


#checking if the length of list and appending data into that list  
  
    if len(transformed_data_list)<=10:
        transformed_data_list.append(transformed_data)
        time.sleep(3)
        etl_weather_data(full_url)
    else:
        df_data = pd.DataFrame(transformed_data_list)
            # print(df_data)

        df_data.to_csv("current_weather_data_Mumbai_{}.csv".format(currentTime), index = False)

# Calling Main function
if __name__ == '__main__':
    etl_weather_data(full_url)