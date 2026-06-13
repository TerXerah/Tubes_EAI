import os
import time
import xml.etree.ElementTree as ET

import mysql.connector
from flask import Flask, request, jsonify, Response

app = Flask(__name__)


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response


DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql-inventory"),
    "user": os.environ.get("MYSQL_USER", "inventory_user"),
    "password": os.environ.get("MYSQL_PASS", "inventory_pass"),
    "database": os.environ.get("MYSQL_DB", "inventory_db"),
}


def get_connection(retries=15, delay=2):
    """Connect to MySQL with retries (MySQL container takes time to be ready)."""
    for attempt in range(retries):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except mysql.connector.Error as err:
            print(f"[Inventory] MySQL not ready ({err}), retry {attempt + 1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to MySQL after retries")


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock (
            sku_code VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            quantity INT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_movements (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sku_code VARCHAR(50) NOT NULL,
            quantity_deducted INT NOT NULL,
            reference_id VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    #sample
    cur.execute("""
        INSERT IGNORE INTO stock (sku_code, product_name, quantity) VALUES
        ('PRD-001', 'Cumi Hitam Pak Kris', 100),
        ('PRD-002', 'Oseng Oseng Biawak', 100),
        ('PRD-003', 'Tiket Whoosh', 100)
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[Inventory] Database initialized")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "inventory-service"})


@app.route("/stock", methods=["GET"])
def get_stock():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM stock ORDER BY sku_code")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows)


@app.route("/stock/movements", methods=["GET"])
def get_movements():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM stock_movements ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(rows, default=str)


SOAP_NS = "http://inventory.example.com/soap"

SOAP_RESPONSE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DeductStockResponse xmlns="{ns}">
      <status>{status}</status>
      <sku_code>{sku_code}</sku_code>
      <remaining_quantity>{remaining}</remaining_quantity>
      <reference_id>{reference_id}</reference_id>
    </DeductStockResponse>
  </soap:Body>
</soap:Envelope>"""


@app.route("/soap/inventory", methods=["POST"])
def soap_inventory():
    """
    SOAP/XML endpoint - this is the heterogeneous-format integration point.

    Expected request body (sent by Integration Service's Message Translator
    after converting JSON CDM -> SOAP XML):

    <soap:Envelope>
      <soap:Body>
        <DeductStock>
          <sku_code>PRD-001</sku_code>
          <quantity_deducted>2</quantity_deducted>
          <reference_id>TXN-001</reference_id>
        </DeductStock>
      </soap:Body>
    </soap:Envelope>
    """
    try:
        xml_body = request.data.decode("utf-8")
        root = ET.fromstring(xml_body)

        # Find DeductStock element regardless of namespace
        deduct = None
        for elem in root.iter():
            if elem.tag.endswith("DeductStock"):
                deduct = elem
                break

        if deduct is None:
            return Response(
                SOAP_RESPONSE_TEMPLATE.format(
                    ns=SOAP_NS, status="ERROR", sku_code="", remaining=0, reference_id=""
                ),
                status=400,
                mimetype="text/xml",
            )

        def get_field(parent, name):
            for child in parent:
                if child.tag.endswith(name):
                    return child.text
            return None

        sku_code = get_field(deduct, "sku_code")
        quantity_deducted = int(get_field(deduct, "quantity_deducted"))
        reference_id = get_field(deduct, "reference_id")

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT quantity FROM stock WHERE sku_code = %s FOR UPDATE", (sku_code,))
        row = cur.fetchone()

        if row is None:
            cur.close()
            conn.close()
            return Response(
                SOAP_RESPONSE_TEMPLATE.format(
                    ns=SOAP_NS, status="NOT_FOUND", sku_code=sku_code, remaining=0,
                    reference_id=reference_id
                ),
                status=404,
                mimetype="text/xml",
            )

        new_qty = max(0, row["quantity"] - quantity_deducted)
        cur.execute("UPDATE stock SET quantity = %s WHERE sku_code = %s", (new_qty, sku_code))
        cur.execute(
            "INSERT INTO stock_movements (sku_code, quantity_deducted, reference_id) VALUES (%s, %s, %s)",
            (sku_code, quantity_deducted, reference_id),
        )
        conn.commit()
        cur.close()
        conn.close()

        print(f"[Inventory] Stock deducted via SOAP: {sku_code} -{quantity_deducted} "
              f"(ref={reference_id}), remaining={new_qty}")

        return Response(
            SOAP_RESPONSE_TEMPLATE.format(
                ns=SOAP_NS, status="SUCCESS", sku_code=sku_code,
                remaining=new_qty, reference_id=reference_id
            ),
            status=200,
            mimetype="text/xml",
        )

    except Exception as e:
        print(f"[Inventory] SOAP processing error: {e}")
        return Response(
            SOAP_RESPONSE_TEMPLATE.format(
                ns=SOAP_NS, status="ERROR", sku_code="", remaining=0, reference_id=""
            ),
            status=500,
            mimetype="text/xml",
        )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8002)))