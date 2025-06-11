from flask import Flask, render_template, jsonify
import pandas as pd
import json
import os
from transformer import (
    apply_transformations,
    parse_transform_config,
    build_dependency_map,
    detect_cycles,
    topological_sort
)

app = Flask(__name__)

def load_input_data():
    if not os.path.exists("input_data.csv"):
        raise FileNotFoundError("input_data.csv not found.")
    df = pd.read_csv("input_data.csv")
    if "subscription_date" in df.columns:
        df["subscription_date"] = pd.to_datetime(df["subscription_date"])
    return df

@app.route("/")
def index():
    config = parse_transform_config()
    derived_columns = config["derivedcolumns"]
    input_df = load_input_data()
    original_df = input_df.copy()
    transformed_steps = apply_transformations(input_df.copy(), derived_columns, return_steps=True)
    dep_map = build_dependency_map(config)
    plan = topological_sort(dep_map, config)
    return render_template("table.html", steps=transformed_steps, plan=plan, original_df=original_df, dependencies=dep_map)

@app.route("/dag")
def dag():
    config = parse_transform_config()
    dep_map = build_dependency_map(config)
    if detect_cycles(dep_map):
        return jsonify({"error": "Cycle detected in dependencies!"}), 400
    execution_plan = topological_sort(dep_map, config)
    input_df = load_input_data()
    transformed_steps = apply_transformations(input_df.copy(), config["derivedcolumns"], return_steps=True)
    return render_template("dag.html", plan=execution_plan, dependencies=dep_map, steps=transformed_steps, original_df=input_df)

if __name__ == "__main__":
    app.run(debug=True)