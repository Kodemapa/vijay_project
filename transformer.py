import pandas as pd
import re
import uuid
import json
from collections import defaultdict, deque

def parse_transform_config(path="transform_config.json"):
    with open(path, "r") as f:
        return json.load(f)

def extract_dependencies(expr, source_columns):
    tokens = re.findall(r"\b[a-zA-Z_]+\b", expr)
    return [token for token in tokens if token in source_columns]

def build_dependency_map(config):
    source_columns = set(config["source"]["schema"])
    derived_columns = config["derivedcolumns"]
    dep_map = {}
    for item in derived_columns:
        col = item["column_name"]
        exprs = [rule["expr"] for rule in item["rules"] if "expr" in rule]
        deps = set()
        for expr in exprs:
            deps.update(extract_dependencies(expr, source_columns))
        dep_map[col] = list(deps)
    return dep_map

def detect_cycles(dep_map):
    visited, stack = set(), set()

    def dfs(v):
        visited.add(v)
        stack.add(v)
        for neighbor in dep_map.get(v, []):
            if neighbor not in visited:
                if dfs(neighbor):
                    return True
            elif neighbor in stack:
                return True
        stack.remove(v)
        return False

    for node in dep_map:
        if node not in visited:
            if dfs(node):
                return True
    return False

def topological_sort(dep_map, config):
    indegree = defaultdict(int)
    reverse_graph = defaultdict(list)
    levels = defaultdict(list)

    for col, deps in dep_map.items():
        for dep in deps:
            reverse_graph[dep].append(col)
            indegree[col] += 1

    q = deque()
    for col in dep_map:
        if indegree[col] == 0:
            q.append((col, 0))

    level_map = {}
    while q:
        node, lvl = q.popleft()
        level_map[node] = lvl
        levels[lvl].append(node)
        for neighbor in reverse_graph.get(node, []):
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                q.append((neighbor, lvl + 1))

    # Sort by sequence
    seq_map = {
        item["column_name"]: item["rules"][0]["sequence"]
        for item in config["derivedcolumns"]
        if item["column_name"] in levels[0]
    }
    levels[0].sort(key=lambda col: seq_map.get(col, 999))

    return {f"Level {lvl}": cols for lvl, cols in levels.items()}

def generate_uuid():
    return str(uuid.uuid4())

def safe_eval(expr, row):
    local_env = row.to_dict()
    local_env["generate_uuid"] = generate_uuid
    local_env["like"] = lambda val, pattern: pattern.replace("%", "") in val if isinstance(val, str) else False
    local_env["replace"] = lambda val, a, b: val.replace(a, b) if isinstance(val, str) else val
    local_env["to_date"] = lambda date_str, fmt: pd.to_datetime(date_str)
    try:
        return eval(expr, {"__builtins__": {}}, local_env)
    except Exception as e:
        print(f"[Eval Error] {expr} → {e}")
        return None

def apply_transformations(df, derived_columns, return_steps=False):
    source_columns = df.columns.tolist()
    dep_map = build_dependency_map({"source": {"schema": source_columns}, "derivedcolumns": derived_columns})
    levels = topological_sort(dep_map, {"derivedcolumns": derived_columns})
    expr_map = {col["column_name"]: col["rules"][0]["expr"] for col in derived_columns}

    step_data = []
    for level in sorted(levels.keys()):
        cols = levels[level]
        for col in cols:
            expr = expr_map.get(col)
            if expr:
                df[col] = df.apply(lambda row: safe_eval(expr, row), axis=1)
        step_data.append({"level": f"Level {level}", "df": df.copy()})

    return step_data if return_steps else df
