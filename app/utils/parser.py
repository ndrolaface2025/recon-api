import pandas as pd
import pdfplumber
import io

def parse_file(file_bytes, filename):
    filename = filename.lower()

    if filename.endswith(".csv") or filename.endswith(".txt"):
        return pd.read_csv(io.BytesIO(file_bytes))

    if filename.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(file_bytes))

    if filename.endswith(".pdf"):
        rows = []
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    rows.extend(table)

        if len(rows) < 2:
            raise ValueError("No table found in PDF")

        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df

    raise ValueError("Unsupported file type")
