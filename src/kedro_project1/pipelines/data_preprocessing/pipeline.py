"""
This is a boilerplate pipeline 'data_preprocessing'
generated using Kedro 0.19.3
"""

# Import necessary libraries
from kedro.pipeline import Pipeline, pipeline, node

from .nodes import (
    preprocess_patients,
    preprocess_patient_gender,
    preprocess_symptoms,
    preprocess_medications,
    preprocess_encounters,
    preprocess_conditions,
    create_patient_id_table,
    create_encounter_id_table,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_patients,
                inputs="patients",
                outputs="preprocessed_patients",
                name="preprocess_patients_node",
            ),
            node(
                func=preprocess_patient_gender,
                inputs="patient_gender",
                outputs="preprocessed_patient_gender",
                name="preprocess_patient_gender_node",
            ),
            node(
                func=preprocess_symptoms,
                inputs="symptoms",
                outputs="preprocessed_symptoms",
                name="preprocess_symptoms_node",
            ),
            node(
                func=preprocess_medications,
                inputs="medications",
                outputs="preprocessed_medications",
                name="preprocess_medications_node",
            ),
            node(
                func=preprocess_encounters,
                inputs="encounters",
                outputs="preprocessed_encounters",
                name="preprocess_encounters_node",
            ),
            node(
                func=preprocess_conditions,
                inputs="conditions",
                outputs="preprocessed_conditions",
                name="preprocess_conditions_node",
            ),
            node(
                func=create_patient_id_table,
                inputs=["preprocessed_patients", "preprocessed_patient_gender"],
                outputs="patients_table",
                name="patients_table_node",
            ),
            node(
                func=create_patient_id_table,
                inputs=["patients_table", "preprocessed_medications"],
                outputs="patient_medications_table",
                name="patient_medications_table_node",
            ),
            node(
                func=create_patient_id_table,
                inputs=["patient_medications_table", "preprocessed_symptoms"],
                outputs="patient_symptoms_medications_master_table",
                name="patient_symptoms_medications_master_table_node",
            ),
            node(
                func=create_encounter_id_table,
                inputs=["preprocessed_conditions", "preprocessed_encounters"],
                outputs="conditions_encounters_table",
                name="conditions_encounters_table_node",
            ),
        ]
    )
