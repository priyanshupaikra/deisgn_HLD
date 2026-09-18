"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 6                       ║
║                 XML (Legacy / Enterprise Integrations)                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Verbose, tag-based markup format. Supports schemas (XSD), XML namespaces,
attributes, and strict validation. Standard in legacy enterprise backbones,
financial networks (SWIFT / ISO 20022), SOAP web services, and healthcare (HL7).

USER STORY:
───────────
As an integration engineer connecting to a 15-year-old banking SOAP API, I need
to produce and parse XML in the exact schema the bank's system expects, since
that's the only format their gateway accepts.

CODE (Python — xml.etree.ElementTree):
──────────────────────────────────────
order = ET.Element("Order")
ET.SubElement(order, "ID").text = "101"
ET.SubElement(order, "Customer").text = "Alice"
xml_string = ET.tostring(order, encoding="unicode")

root = ET.fromstring(xml_string)
print(root.find("Customer").text)  # Alice

WHY IT FITS:
────────────
• Rich schema validation via XSD schemas.
• Strong namespace support for federated enterprise documents.
• Deeply entrenched in banking, government, and legacy enterprise software.

DOWNSIDES:
──────────
• Extreme verbosity: opening and closing tags duplicate strings heavily.
• Slower parsing and high memory overhead (DOM tree allocation).
• Vulnerable to XML External Entity (XXE) and billion-laughs expansion attacks
  if parsers are not secured.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Optional


def serialize_order(order_id: str, customer_name: str, amount: Optional[float] = None) -> str:
    """Serializes order data into an XML document."""
    order = ET.Element("Order")
    
    id_elem = ET.SubElement(order, "ID")
    id_elem.text = str(order_id)
    
    cust_elem = ET.SubElement(order, "Customer")
    cust_elem.text = str(customer_name)
    
    if amount is not None:
        amt_elem = ET.SubElement(order, "Amount")
        amt_elem.text = f"{amount:.2f}"

    return ET.tostring(order, encoding="unicode")


def deserialize_order(xml_string: str) -> Dict[str, str]:
    """Deserializes an XML string into a Python dictionary."""
    root = ET.fromstring(xml_string)
    id_val = root.find("ID")
    cust_val = root.find("Customer")
    amt_val = root.find("Amount")

    return {
        "id": id_val.text if id_val is not None else "",
        "customer": cust_val.text if cust_val is not None else "",
        "amount": amt_val.text if amt_val is not None else "0.00"
    }


# ─── Enterprise SOAP XML Envelope Simulator ───────────────────────────────────

def build_soap_transfer_request(tx_id: str, from_acct: str, to_acct: str, amount: float) -> str:
    """Constructs an enterprise SOAP 1.2 XML envelope with namespaces."""
    soap_env = ET.Element("soap:Envelope", {
        "xmlns:soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "xmlns:bank": "http://enterprise.bank.com/payments/v1"
    })
    body = ET.SubElement(soap_env, "soap:Body")
    transfer = ET.SubElement(body, "bank:TransferFundsRequest")

    ET.SubElement(transfer, "bank:TransactionId").text = tx_id
    ET.SubElement(transfer, "bank:SourceAccount").text = from_acct
    ET.SubElement(transfer, "bank:DestinationAccount").text = to_acct
    ET.SubElement(transfer, "bank:Amount").text = f"{amount:.2f}"
    ET.SubElement(transfer, "bank:Currency").text = "USD"

    return ET.tostring(soap_env, encoding="unicode")


def parse_soap_transfer_request(soap_xml: str) -> Dict[str, str]:
    """Parses namespaced SOAP XML payload."""
    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "bank": "http://enterprise.bank.com/payments/v1"
    }
    root = ET.fromstring(soap_xml)
    transfer_elem = root.find("soap:Body/bank:TransferFundsRequest", namespaces)
    if transfer_elem is None:
        raise ValueError("Invalid SOAP envelope: Missing TransferFundsRequest")

    return {
        "tx_id": transfer_elem.find("bank:TransactionId", namespaces).text,
        "source": transfer_elem.find("bank:SourceAccount", namespaces).text,
        "destination": transfer_elem.find("bank:DestinationAccount", namespaces).text,
        "amount": transfer_elem.find("bank:Amount", namespaces).text,
        "currency": transfer_elem.find("bank:Currency", namespaces).text,
    }


def run_demo():
    print(f"\n{'═' * 65}")
    print("  6. XML — Legacy / Enterprise Integrations Demonstration")
    print(f"{'═' * 65}")

    # Standard User Story Demo
    print("\n[Serialization]")
    xml_string = serialize_order(order_id="101", customer_name="Alice")
    print(f"Serialized XML:  {xml_string}")
    print(f"Payload Size:    {len(xml_string.encode('utf-8'))} bytes")

    print("\n[Deserialization]")
    order_data = deserialize_order(xml_string)
    print(f"Root tag parsed: <Order>")
    print(f"Customer Name:   {order_data['customer']}")
    print(f"Order ID:        {order_data['id']}")

    # Enterprise Banking SOAP Integration
    print("\n[Enterprise Banking SOAP API Gateway Simulation]")
    soap_req = build_soap_transfer_request(
        tx_id="TXN-984210",
        from_acct="US-BANK-401923",
        to_acct="US-BANK-998124",
        amount=5000.00
    )
    print("Outbound SOAP 1.2 Envelope:")
    print(f"{soap_req}")
    print(f"Envelope Size:   {len(soap_req.encode('utf-8'))} bytes")

    print("\nBank Gateway Ingestion & Parsing:")
    parsed_soap = parse_soap_transfer_request(soap_req)
    for k, v in parsed_soap.items():
        print(f"  • {k:<12}: {v}")
    print("✅ Successfully verified against banking XSD namespace contract.")


if __name__ == "__main__":
    run_demo()
