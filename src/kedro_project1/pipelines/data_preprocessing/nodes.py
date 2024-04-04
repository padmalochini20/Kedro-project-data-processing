"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.3
"""

# import necessary libraries
import pandas as pd


def _remove_null_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(axis=1, how="any")


def _convert_to_datetime(x: pd.Series) -> pd.Series:
    return pd.to_datetime(x, errors="coerce")


def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    # Column names are converted to lower case and space is replaced with underscore '_'
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df


def _rename_columns(df: pd.DataFrame, column_map) -> pd.DataFrame:
    return df.rename(columns=column_map)


def _merge_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, primary_key, how="inner"
) -> pd.DataFrame:
    merged_df = df1.merge(df2, on=primary_key, how=how, copy=False)

    # common columns from right df
    drop_cols = [col for col in merged_df.columns if col.endswith("_y")]
    merged_df = merged_df.drop(drop_cols, axis=1)

    # Remove '_x' from the common columns
    merged_df = merged_df.rename(
        columns={col: col.replace("_x", "") for col in merged_df.columns}
    )

    return merged_df


def _extract_symptoms_values(df: pd.DataFrame) -> pd.DataFrame:
    # Split the 'SYMPTOMS' column into separate parts
    split_data = df["symptoms"].str.split(";")

    # Initialize lists to store the values of each symptoms
    rash = []
    joint_pain = []
    fatigue = []
    fever = []

    # Iterate over each 'SYMPTOMS' value
    for data in split_data:
        for item in data:
            symptom, value = item.split(":")
            if symptom == "Rash":
                rash.append(int(value))
            if symptom == "Joint Pain":
                joint_pain.append(int(value))
            if symptom == "Fatigue":
                fatigue.append(int(value))
            if symptom == "Fever":
                fever.append(int(value))

    df["rash"] = rash
    df["joint_pain"] = joint_pain
    df["fatigue"] = fatigue
    df["fever"] = fever

    df = df.drop(["symptoms"], axis=1)

    return df


def _drop_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    return df.drop(columns, axis=1)


def _standardize_columns_values(df: pd.DataFrame, columns) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.capitalize()
    return df


def _filter_lupus(df: pd.DataFrame, column) -> pd.DataFrame:
    return df[df[column] == "Lupus erythematosus"].reset_index(drop=True)


def preprocess_patients(df: pd.DataFrame) -> pd.DataFrame:
    df = _remove_null_columns(df)
    df = _standardize_column_names(df)
    df = _drop_columns(df, ["birthdate"])
    return df


def preprocess_patient_gender(df: pd.DataFrame) -> pd.DataFrame:
    df = _standardize_column_names(df)
    df = _rename_columns(df, {"id": "patient_id"})
    return df


def preprocess_symptoms(df: pd.DataFrame) -> pd.DataFrame:
    df = _remove_null_columns(df)
    df = _standardize_column_names(df)
    df = _filter_lupus(df, "pathology")
    df = _extract_symptoms_values(df)
    df = _rename_columns(df, {"patient": "patient_id"})
    df = _drop_columns(df, ["pathology", "num_symptoms"])
    return df


def preprocess_medications(df: pd.DataFrame) -> pd.DataFrame:
    df = _standardize_column_names(df)
    df["start"] = _convert_to_datetime(df["start"])
    df["stop"] = _convert_to_datetime(df["stop"])
    df = _filter_lupus(df, "reasondescription")
    df = _standardize_columns_values(df, ["description"])
    df = _drop_columns(df, ["reasoncode", "reasondescription"])
    df = _rename_columns(
        df,
        {
            "patient": "patient_id",
            "code": "medical_code",
            "description": "medical_description",
            "start": "medical_start_date",
            "stop": "medical_stop_date",
        },
    )
    return df


def preprocess_encounters(df: pd.DataFrame) -> pd.DataFrame:
    df = _standardize_column_names(df)
    df = _filter_lupus(df, "reasondescription")
    df["start"] = _convert_to_datetime(df["start"])
    df["stop"] = _convert_to_datetime(df["stop"])
    df = _rename_columns(
        df,
        {
            "patient": "patient_encounter_id",
            "id": "encounter_id",
            "start": "encounter_start_date",
            "stop": "encounter_stop_date",
            "code": "encounter_code",
            "description": "encounter_description",
            "provider": "encounter_provider",
            "organization": "encounter_organization",
            "payer": "encounter_payer",
        },
    )
    df = _drop_columns(df, ["reasoncode", "reasondescription"])
    return df


def preprocess_conditions(df: pd.DataFrame) -> pd.DataFrame:
    df = _remove_null_columns(df)
    df = _standardize_column_names(df)
    df = _rename_columns(
        df,
        {
            "patient": "patient_encounter_id",
            "encounter": "encounter_id",
            "start": "condition_start_date",
        },
    )
    df = _standardize_columns_values(df, ["description"])
    df = _filter_lupus(df, "description")
    df = _drop_columns(df, ["code", "description"])
    return df


def create_patient_id_table(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return _merge_dataframes(df1, df2, primary_key="patient_id")


def create_encounter_id_table(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return _merge_dataframes(df1, df2, primary_key="encounter_id")
