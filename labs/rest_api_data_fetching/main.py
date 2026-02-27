#!/usr/bin/env python3
"""
REST API Basics Lab.

Demonstrates:
- Using RandomUser API via Python library
- Consuming REST APIs using requests
- Parsing JSON responses
- Converting API results into pandas DataFrames
"""

from randomuser import RandomUser
import pandas as pd
import requests


# --------------------------------------------------
# RANDOM USER API
# --------------------------------------------------

def get_random_users(n_users: int = 10) -> pd.DataFrame:
    """
    Generate random users and return selected fields as a DataFrame.
    """
    users = []

    for user in RandomUser.generate_users(n_users):
        users.append(
            {
                "Name": user.get_full_name(),
                "Gender": user.get_gender(),
                "City": user.get_city(),
                "State": user.get_state(),
                "Email": user.get_email(),
                "DOB": user.get_dob(),
                "Picture": user.get_picture(),
            }
        )

    return pd.DataFrame(users)


# --------------------------------------------------
# FRUITYVICE API
# --------------------------------------------------

def fetch_fruit_data() -> pd.DataFrame:
    """
    Fetch fruit information from Fruityvice API.
    """
    url = "https://fruityvice.com/api/fruit/all"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    results = response.json()

    return pd.json_normalize(results)


# --------------------------------------------------
# OFFICIAL JOKE API
# --------------------------------------------------

def fetch_jokes() -> pd.DataFrame:
    """
    Fetch random jokes from Official Joke API.
    """
    url = "https://official-joke-api.appspot.com/jokes/ten"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    jokes = response.json()

    dataframe = pd.DataFrame(jokes)
    dataframe = dataframe.drop(columns=["type", "id"])

    return dataframe


# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------

def main() -> None:
    """Run API demonstrations."""

    print("\nGenerating Random Users...")
    df_users = get_random_users()
    print(df_users.head())

    print("\nFetching Fruit Data...")
    df_fruits = fetch_fruit_data()
    print(df_fruits.head())

    banana_calories = df_fruits.loc[
        df_fruits["name"] == "Banana",
        "nutritions.calories",
    ].iloc[0]

    print(f"\nCalories in a banana: {banana_calories}")

    print("\nFetching Jokes...")
    df_jokes = fetch_jokes()
    print(df_jokes.head())


if __name__ == "__main__":
    main()
