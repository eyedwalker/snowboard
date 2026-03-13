"""Pre-built analytics queries for common eyecare business questions.
Uses actual column names from DATAWAREHOUSE views.
"""

from src.agent.tools.registry import tool_definition
from src.connectors.snowflake_connector import execute_query


@tool_definition(
    name="revenue_summary",
    description="Get revenue summary with totals, averages, and trends from InvoiceDetail (Amount, Price) joined with InvoiceSummary.",
    parameters={
        "properties": {
            "start_date": {
                "type": "string",
                "description": "Start date (YYYY-MM-DD). If omitted, returns all data.",
            },
            "end_date": {
                "type": "string",
                "description": "End date (YYYY-MM-DD). If omitted, defaults to today.",
            },
            "group_by": {
                "type": "string",
                "description": "Group results by: 'month', 'quarter', 'year'",
                "default": "month",
            },
        },
        "required": [],
    },
)
def revenue_summary(start_date=None, end_date=None, group_by="month"):
    date_filter = ""
    if start_date:
        date_filter += f' AND id."LastOperationDateTime" >= \'{start_date}\''
    if end_date:
        date_filter += f' AND id."LastOperationDateTime" <= \'{end_date}\''

    if group_by == "quarter":
        group_expr = """DATE_TRUNC('QUARTER', id."LastOperationDateTime")"""
    elif group_by == "year":
        group_expr = """DATE_TRUNC('YEAR', id."LastOperationDateTime")"""
    else:
        group_expr = """DATE_TRUNC('MONTH', id."LastOperationDateTime")"""

    sql = f"""
        SELECT {group_expr} AS "Period",
               COUNT(*) AS "LineItems",
               SUM(id."Amount") AS "TotalRevenue",
               AVG(id."Amount") AS "AvgLineAmount"
        FROM DATAWAREHOUSE."InvoiceDetail" id
        WHERE id."Amount" IS NOT NULL
          AND id."LastOperationDateTime" IS NOT NULL
          {date_filter}
        GROUP BY "Period"
        ORDER BY "Period"
    """
    df = execute_query(sql, limit=500)
    if df.empty:
        return {"message": "No revenue data found for the specified period", "data": []}
    return {
        "summary": {
            "total_revenue": float(df["TotalRevenue"].sum()) if "TotalRevenue" in df.columns else 0,
            "total_line_items": int(df["LineItems"].sum()) if "LineItems" in df.columns else 0,
        },
        "data": df.to_dict(orient="records"),
    }


@tool_definition(
    name="patient_summary",
    description="Get patient demographics summary: total count, office distribution, gender breakdown from the Patient view.",
    parameters={
        "properties": {
            "office_id": {
                "type": "string",
                "description": "Filter by HomeOfficeKey (optional)",
            },
        },
        "required": [],
    },
)
def patient_summary(office_id=None):
    where = f'WHERE p."HomeOfficeKey" = \'{office_id}\'' if office_id else ""
    sql = f"""
        SELECT COUNT(*) AS "TotalPatients",
               COUNT(DISTINCT p."HomeOfficeKey") AS "Offices",
               SUM(CASE WHEN p."Sex" = 'M' THEN 1 ELSE 0 END) AS "Male",
               SUM(CASE WHEN p."Sex" = 'F' THEN 1 ELSE 0 END) AS "Female",
               SUM(CASE WHEN p."IsInActive" = TRUE THEN 1 ELSE 0 END) AS "Inactive"
        FROM DATAWAREHOUSE."Patient" p
        {where}
    """
    df = execute_query(sql)
    if df.empty:
        return {"message": "No patient data found"}
    return df.to_dict(orient="records")[0]


@tool_definition(
    name="top_products",
    description="Get top products/items by revenue or quantity sold. Joins InvoiceDetail with Item on ItemId.",
    parameters={
        "properties": {
            "n": {
                "type": "integer",
                "description": "Number of top products to return",
                "default": 10,
            },
            "metric": {
                "type": "string",
                "description": "'revenue' or 'quantity'",
                "default": "revenue",
            },
        },
        "required": [],
    },
)
def top_products(n=10, metric="revenue"):
    order_col = "TotalRevenue" if metric == "revenue" else "TotalQuantity"
    sql = f"""
        SELECT it."Name" AS "Product",
               it."ItemType_ItemType" AS "ItemType",
               SUM(id."Quantity") AS "TotalQuantity",
               SUM(id."Amount") AS "TotalRevenue"
        FROM DATAWAREHOUSE."InvoiceDetail" id
        JOIN DATAWAREHOUSE."Item" it ON id."ItemId" = it."ItemId"
            AND id."_Customer" = it."_Customer"
        WHERE it."Name" IS NOT NULL
        GROUP BY it."Name", it."ItemType_ItemType"
        ORDER BY "{order_col}" DESC
        LIMIT {n}
    """
    df = execute_query(sql, limit=n)
    if df.empty:
        return {"message": "No product data found", "data": []}
    return {"data": df.to_dict(orient="records")}


@tool_definition(
    name="appointment_utilization",
    description="Get appointment scheduling metrics: total appointments, show/no-show/canceled counts by status.",
    parameters={
        "properties": {
            "office_id": {
                "type": "string",
                "description": "Filter by OfficeId (optional)",
            },
            "start_date": {
                "type": "string",
                "description": "Start date (YYYY-MM-DD)",
            },
        },
        "required": [],
    },
)
def appointment_utilization(office_id=None, start_date=None):
    where_parts = []
    if office_id:
        where_parts.append(f'sa."OfficeId" = \'{office_id}\'')
    if start_date:
        where_parts.append(f'sa."Date" >= \'{start_date}\'')
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT sa."DerivedStatus" AS "Status",
               COUNT(*) AS "Count",
               SUM(CASE WHEN sa."IsCanceled" = TRUE THEN 1 ELSE 0 END) AS "Canceled"
        FROM DATAWAREHOUSE."ScheduledAppointment" sa
        {where}
        GROUP BY sa."DerivedStatus"
        ORDER BY "Count" DESC
    """
    df = execute_query(sql, limit=100)
    if df.empty:
        return {"message": "No appointment data found", "data": []}

    total = int(df["Count"].sum())
    return {
        "total_appointments": total,
        "data": df.to_dict(orient="records"),
    }
