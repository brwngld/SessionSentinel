from io import BytesIO
import os

import pandas as pd
from fpdf import FPDF


def _normalize_pricing_profile(pricing_profile):
    profile = pricing_profile or {}
    mode = str(profile.get("pricing_mode") or "none").strip().lower()
    if mode not in {"none", "automatic", "manual"}:
        mode = "none"
    try:
        fixed_price = float(profile.get("fixed_price") or 0)
    except (TypeError, ValueError):
        fixed_price = 0.0
    line_prices = profile.get("line_prices") if isinstance(profile.get("line_prices"), dict) else {}
    currency_code = str(profile.get("currency_code") or "GHS").strip().upper()
    if currency_code not in {"GHS", "USD"}:
        currency_code = "GHS"
    try:
        manual_rate = float(profile.get("manual_rate")) if profile.get("manual_rate") is not None else None
    except (TypeError, ValueError):
        manual_rate = None
    if manual_rate is not None and manual_rate <= 0:
        manual_rate = None
    conversion_note = str(profile.get("conversion_note") or "").strip()
    return mode, max(0.0, fixed_price), line_prices, currency_code, manual_rate, conversion_note


def _format_money(amount, currency_code="GHS"):
    code = str(currency_code or "GHS").strip().upper()
    if code not in {"GHS", "USD"}:
        code = "GHS"
    try:
        numeric = float(amount or 0)
    except (TypeError, ValueError):
        numeric = 0.0
    return f"{code} {numeric:,.2f}"


def _int_to_words(num):
    ones = [
        "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
        "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen",
    ]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

    def under_thousand(n):
        parts = []
        if n >= 100:
            parts.append(f"{ones[n // 100]} hundred")
            n %= 100
        if n >= 20:
            t = tens[n // 10]
            n %= 10
            if n:
                parts.append(f"{t}-{ones[n]}")
            else:
                parts.append(t)
        elif n > 0:
            parts.append(ones[n])
        return " ".join(parts) if parts else "zero"

    if num == 0:
        return "zero"

    scales = [(1_000_000_000, "billion"), (1_000_000, "million"), (1_000, "thousand")]
    remaining = int(num)
    out = []
    for scale_value, scale_name in scales:
        if remaining >= scale_value:
            chunk = remaining // scale_value
            out.append(f"{under_thousand(chunk)} {scale_name}")
            remaining %= scale_value
    if remaining > 0:
        out.append(under_thousand(remaining))
    return " ".join(out)


def _amount_in_words(amount, currency_code="GHS"):
    code = str(currency_code or "GHS").strip().upper()
    if code not in {"GHS", "USD"}:
        code = "GHS"
    try:
        value = max(0.0, float(amount or 0))
    except (TypeError, ValueError):
        value = 0.0
    whole = int(value)
    cents = int(round((value - whole) * 100))
    if cents == 100:
        whole += 1
        cents = 0
    major_unit = "cedis" if code == "GHS" else "dollars"
    minor_unit = "pesewas" if code == "GHS" else "cents"
    if cents:
        return f"{_int_to_words(whole)} {major_unit} and {_int_to_words(cents)} {minor_unit}".title()
    return f"{_int_to_words(whole)} {major_unit} only".title()


def _convert_total_to_ghs(total_amount, currency_code, manual_rate):
    code = str(currency_code or "GHS").strip().upper()
    if code == "USD" and manual_rate is not None and float(manual_rate) > 0:
        return float(total_amount or 0) * float(manual_rate)
    if code == "GHS":
        return float(total_amount or 0)
    return None


def _resolve_line_amount(mode, fixed_price, line_prices, line_key):
    if mode == "automatic":
        return float(fixed_price)
    if mode == "manual":
        raw = line_prices.get(str(line_key), 0)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def build_account_report_dataframe(df, account_data, resolve_account_columns, normalize_value, pricing_profile=None):
    pricing_mode, fixed_price, line_prices, _currency_code, _manual_rate, _conversion_note = _normalize_pricing_profile(pricing_profile)
    source_indexes = []
    for row in account_data.get("rows", []):
        raw_idx = row.get("source_idx")
        if raw_idx is None or raw_idx == "":
            continue
        try:
            source_indexes.append(int(raw_idx))
        except (TypeError, ValueError):
            continue

    if source_indexes:
        filtered_df = df.loc[df.index.isin(source_indexes)].copy()
        boe_col, user_ref_col, date_col, _imp_code_col, _created_by_col = resolve_account_columns(filtered_df)
        if not boe_col or not user_ref_col:
            raise ValueError("Required columns not found for account export")

        report_df = pd.DataFrame(
            {
                "User Ref": filtered_df[user_ref_col].apply(normalize_value),
                "BOE Number": filtered_df[boe_col].apply(normalize_value),
                "Submission Date": filtered_df[date_col].apply(normalize_value) if date_col else "",
            }
        )
        report_df["_line_key"] = filtered_df.index.astype(str)
    else:
        rows_df = pd.DataFrame(account_data.get("rows", [])).copy()
        report_df = pd.DataFrame(
            {
                "User Ref": rows_df.get("user_ref_raw", pd.Series(dtype="object")).apply(normalize_value),
                "BOE Number": rows_df.get("boe", pd.Series(dtype="object")).apply(normalize_value),
                "Submission Date": rows_df.get("date", pd.Series(dtype="object")).apply(normalize_value),
            }
        )
        if "source_idx" in rows_df.columns:
            report_df["_line_key"] = rows_df["source_idx"].astype(str)
        else:
            report_df["_line_key"] = report_df.index.astype(str)

    if pricing_mode in {"automatic", "manual"}:
        report_df["Amount"] = report_df["_line_key"].apply(
            lambda key: _resolve_line_amount(pricing_mode, fixed_price, line_prices, key)
        )

    parsed_dates = pd.to_datetime(report_df["Submission Date"], errors="coerce", dayfirst=True)
    week_start = parsed_dates.dt.to_period("W-SUN").dt.start_time
    week_end = parsed_dates.dt.to_period("W-SUN").dt.end_time
    report_df["_week_key"] = week_start
    report_df["_week_label"] = week_start.dt.strftime("Week %Y-%m-%d")
    report_df.loc[week_start.notna(), "_week_label"] = (
        "Week "
        + week_start[week_start.notna()].dt.strftime("%d %b %Y")
        + " to "
        + week_end[week_start.notna()].dt.strftime("%d %b %Y")
    )
    report_df.loc[week_start.isna(), "_week_label"] = "Unknown Week"

    return report_df.sort_values(by=["_week_key", "Submission Date", "BOE Number"], na_position="last")


def build_account_pdf_bytes(account_name, report_df, generated_at_text, normalize_value, company_profile=None, app_root_path=None, pricing_profile=None):
    def _pdf_safe(value):
        text = normalize_value(value)
        return text.encode("latin-1", errors="replace").decode("latin-1")

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()

    brand_blue = (23, 70, 125)
    soft_header = (232, 236, 241)
    profile = company_profile or {}
    company_name = str(profile.get("name") or "").strip()
    company_address = str(profile.get("address") or "").strip()
    company_phone = str(profile.get("phone") or "").strip()
    company_logo_path = str(profile.get("logo_path") or "").strip()

    if company_name or company_address or company_phone or company_logo_path:
        if company_logo_path and app_root_path:
            logo_abs = os.path.join(app_root_path, company_logo_path)
            logo_ext = os.path.splitext(logo_abs)[1].lower()
            if os.path.exists(logo_abs) and logo_ext in {".png", ".jpg", ".jpeg"}:
                try:
                    pdf.image(logo_abs, x=10, y=10, w=28)
                except RuntimeError:
                    pass

        pdf.set_font("Helvetica", "B", 13)
        if company_name:
            pdf.cell(0, 7, _pdf_safe(company_name), ln=True)
        pdf.set_font("Helvetica", "", 10)
        if company_address:
            pdf.cell(0, 6, _pdf_safe(company_address), ln=True)
        if company_phone:
            pdf.cell(0, 6, _pdf_safe(f"Tel: {company_phone}"), ln=True)
        pdf.ln(2)

    pdf.set_draw_color(*brand_blue)
    pdf.set_fill_color(*brand_blue)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 9, _pdf_safe(f"Account Report: {account_name}"), ln=True, fill=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, _pdf_safe(f"Generated: {generated_at_text}"), ln=True, fill=True)
    pdf.cell(0, 7, _pdf_safe(f"Rows: {len(report_df)}"), ln=True, fill=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)

    has_amount = "Amount" in report_df.columns
    _mode, _fixed_price, _line_prices, currency_code, manual_rate, conversion_note = _normalize_pricing_profile(pricing_profile)

    def draw_table_header():
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(*soft_header)
        pdf.set_text_color(*brand_blue)
        if has_amount:
            pdf.cell(80, 7, "User Ref", border=1, fill=True)
            pdf.cell(78, 7, "BOE Number", border=1, fill=True)
            pdf.cell(78, 7, "Submission Date", border=1, fill=True)
            pdf.cell(44, 7, "Amount", border=1, ln=True, fill=True)
        else:
            pdf.cell(95, 7, "User Ref", border=1, fill=True)
            pdf.cell(90, 7, "BOE Number", border=1, fill=True)
            pdf.cell(90, 7, "Submission Date", border=1, ln=True, fill=True)
        pdf.set_text_color(0, 0, 0)

    grouped = report_df.groupby("_week_label", sort=False)
    for week_label, group in grouped:
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(*brand_blue)
        pdf.cell(0, 8, _pdf_safe(week_label), ln=True)
        pdf.set_text_color(0, 0, 0)
        draw_table_header()

        pdf.set_font("Helvetica", "", 9)
        for _, row in group.iterrows():
            if has_amount:
                amount = float(row.get("Amount") or 0)
                pdf.cell(80, 6, _pdf_safe(row.get("User Ref", ""))[:44], border=1)
                pdf.cell(78, 6, _pdf_safe(row.get("BOE Number", ""))[:34], border=1)
                pdf.cell(78, 6, _pdf_safe(row.get("Submission Date", ""))[:34], border=1)
                pdf.cell(44, 6, _pdf_safe(_format_money(amount, currency_code)), border=1, ln=True, align="R")
            else:
                pdf.cell(95, 6, _pdf_safe(row.get("User Ref", ""))[:52], border=1)
                pdf.cell(90, 6, _pdf_safe(row.get("BOE Number", ""))[:40], border=1)
                pdf.cell(90, 6, _pdf_safe(row.get("Submission Date", ""))[:40], border=1, ln=True)
        pdf.ln(2)

    if has_amount:
        total_amount = float(report_df["Amount"].sum())
        converted_ghs = _convert_total_to_ghs(total_amount, currency_code, manual_rate)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 8, _pdf_safe(f"Total Amount: {_format_money(total_amount, currency_code)}"), ln=True, align="R")
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 7, _pdf_safe(f"Amount in words: {_amount_in_words(total_amount, currency_code)}"), ln=True, align="R")
        if converted_ghs is not None:
            pdf.cell(0, 7, _pdf_safe(f"Converted Total (GHS): {_format_money(converted_ghs, 'GHS')}"), ln=True, align="R")
            pdf.cell(0, 7, _pdf_safe(f"Converted Amount in words (GHS): {_amount_in_words(converted_ghs, 'GHS')}"), ln=True, align="R")
        if conversion_note:
            pdf.cell(0, 6, _pdf_safe(conversion_note), ln=True, align="R")

    pdf_content = pdf.output(dest="S")
    if isinstance(pdf_content, str):
        pdf_content = pdf_content.encode("latin-1", errors="ignore")
    return BytesIO(pdf_content)


def build_account_view_html(account_name, rows, generated_at_text, company_profile=None, pricing_profile=None):
    pricing_mode, fixed_price, line_prices, currency_code, manual_rate, conversion_note = _normalize_pricing_profile(pricing_profile)
    profile = company_profile or {}
    company_name = str(profile.get("name") or "").strip()
    company_address = str(profile.get("address") or "").strip()
    company_phone = str(profile.get("phone") or "").strip()
    company_logo_path = str(profile.get("logo_path") or "").strip()
    company_logo_html = f'<img src="/{company_logo_path}" alt="Company Logo" style="max-height:72px;max-width:180px;display:block;margin-bottom:8px;">' if company_logo_path else ""
    company_block = ""
    if company_name or company_address or company_phone or company_logo_html:
        company_block = f"""
        <div class="company-header">
            {company_logo_html}
            <h2>{company_name or ''}</h2>
            <p>{company_address or ''}</p>
            <p>{('Tel: ' + company_phone) if company_phone else ''}</p>
        </div>
        """

    html = f"""
    <html>
    <head>
        <title>Account: {account_name}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
            .company-header {{ background: #ffffff; border: 1px solid #dbe4f2; border-radius: 8px; padding: 12px; margin-bottom: 14px; }}
            .company-header h2 {{ margin: 0 0 6px 0; color: #17467d; }}
            .company-header p {{ margin: 3px 0; color: #2d3b4f; }}
            .header {{ background: #17467d; color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
            .header h1 {{ margin: 0 0 5px 0; }}
            .header p {{ margin: 5px 0; font-size: 0.9rem; }}
            table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            th {{ background: #e8ecf1; padding: 12px; text-align: left; font-weight: bold; border-bottom: 2px solid #17467d; }}
            td {{ padding: 10px 12px; border-bottom: 1px solid #e0e0e0; }}
            tr:hover {{ background: #f9f9f9; }}
            .back-link {{ margin-bottom: 20px; }}
            .back-link a {{ color: #17467d; text-decoration: none; font-weight: bold; }}
            .back-link a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="back-link">
            <a href="javascript:history.back()">← Back to Report</a>
        </div>
        {company_block}
        <div class="header">
            <h1>Account: {account_name}</h1>
            <p>Total Rows: {len(rows)}</p>
            <p>Generated: {generated_at_text}</p>
        </div>
        <table>
            <thead>
                <tr>
                    <th>User Ref</th>
                    <th>BOE Number</th>
                    <th>IMP/EXP Code</th>
                    <th>Created By</th>
                    <th>Submission Date</th>
                    <th>Amount</th>
                </tr>
            </thead>
            <tbody>
    """

    total_amount = 0.0
    for row in rows:
        line_key = str(row.get("source_idx") if row.get("source_idx") is not None else "")
        amount = _resolve_line_amount(pricing_mode, fixed_price, line_prices, line_key)
        total_amount += amount
        html += f"""
                <tr>
                    <td>{row.get('raw_user_ref', '')}</td>
                    <td>{row.get('boe', '')}</td>
                    <td>{row.get('imp_code', '')}</td>
                    <td>{row.get('created_by', '')}</td>
                    <td>{row.get('date', '') or row.get('submission_date', '')}</td>
                    <td style="text-align:right;">{_format_money(amount, currency_code)}</td>
                </tr>
        """

    converted_ghs = _convert_total_to_ghs(total_amount, currency_code, manual_rate)

    html += """
            </tbody>
            <tfoot>
                <tr>
                    <td colspan="5" style="font-weight:700;text-align:right;">Total Amount</td>
                    <td style="text-align:right;font-weight:700;">""" + f"{_format_money(total_amount, currency_code)}" + """</td>
                </tr>
                <tr>
                    <td colspan="6" style="font-size:0.9rem;color:#1b3555;"><strong>Amount in words:</strong> """ + f"{_amount_in_words(total_amount, currency_code)}" + """</td>
                </tr>
                """ + (
                    "<tr><td colspan=\"5\" style=\"font-weight:700;text-align:right;\">Converted Total (GHS)</td>"
                    f"<td style=\"text-align:right;font-weight:700;\">{_format_money(converted_ghs, 'GHS')}</td></tr>"
                    f"<tr><td colspan=\"6\" style=\"font-size:0.9rem;color:#1b3555;\"><strong>Converted Amount in words (GHS):</strong> {_amount_in_words(converted_ghs, 'GHS')}</td></tr>"
                    if converted_ghs is not None else ""
                ) + """
            </tfoot>
        </table>
        """ + (f"<p style=\"margin-top:8px;color:#2d3b4f;\"><strong>Conversion:</strong> {conversion_note}</p>" if conversion_note else "") + """
    </body>
    </html>
    """
    return html


def build_all_accounts_pdf_bytes(account_report, generated_at_text, normalize_value, company_profile=None, app_root_path=None, pricing_by_account=None):
    def _pdf_safe(value):
        text = normalize_value(value)
        return text.encode("latin-1", errors="replace").decode("latin-1")

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()

    brand_blue = (23, 70, 125)
    soft_header = (232, 236, 241)
    profile = company_profile or {}
    company_name = str(profile.get("name") or "").strip()
    company_address = str(profile.get("address") or "").strip()
    company_phone = str(profile.get("phone") or "").strip()
    company_logo_path = str(profile.get("logo_path") or "").strip()

    accounts = account_report.get("accounts", [])

    if company_name or company_address or company_phone or company_logo_path:
        if company_logo_path and app_root_path:
            logo_abs = os.path.join(app_root_path, company_logo_path)
            logo_ext = os.path.splitext(logo_abs)[1].lower()
            if os.path.exists(logo_abs) and logo_ext in {".png", ".jpg", ".jpeg"}:
                try:
                    pdf.image(logo_abs, x=10, y=10, w=28)
                except RuntimeError:
                    pass

        pdf.set_font("Helvetica", "B", 13)
        if company_name:
            pdf.cell(0, 7, _pdf_safe(company_name), ln=True)
        pdf.set_font("Helvetica", "", 10)
        if company_address:
            pdf.cell(0, 6, _pdf_safe(company_address), ln=True)
        if company_phone:
            pdf.cell(0, 6, _pdf_safe(f"Tel: {company_phone}"), ln=True)
        pdf.ln(2)

    pdf.set_draw_color(*brand_blue)
    pdf.set_fill_color(*brand_blue)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 9, _pdf_safe("Account Groups Report"), ln=True, fill=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, _pdf_safe(f"Generated: {generated_at_text}"), ln=True, fill=True)
    pdf.cell(0, 7, _pdf_safe(f"Total Accounts: {len(accounts)}"), ln=True, fill=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)

    pricing_by_account = pricing_by_account or {}

    def draw_table_header(has_amount):
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(*soft_header)
        pdf.set_text_color(*brand_blue)
        if has_amount:
            pdf.cell(82, 7, "User Ref", border=1, fill=True)
            pdf.cell(70, 7, "BOE Number", border=1, fill=True)
            pdf.cell(50, 7, "IMP/EXP", border=1, fill=True)
            pdf.cell(40, 7, "Created By", border=1, fill=True)
            pdf.cell(36, 7, "Amount", border=1, ln=True, fill=True)
        else:
            pdf.cell(95, 7, "User Ref", border=1, fill=True)
            pdf.cell(85, 7, "BOE Number", border=1, fill=True)
            pdf.cell(55, 7, "IMP/EXP", border=1, fill=True)
            pdf.cell(42, 7, "Created By", border=1, ln=True, fill=True)
        pdf.set_text_color(0, 0, 0)

    for account in accounts:
        rows = account.get("rows", [])
        pricing_mode, fixed_price, line_prices, currency_code, manual_rate, conversion_note = _normalize_pricing_profile(pricing_by_account.get(str(account.get("name", ""))))
        has_amount = pricing_mode in {"automatic", "manual"}
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(*brand_blue)
        pdf.cell(0, 8, _pdf_safe(f"Account: {account.get('name', '')} ({len(rows)} rows)"), ln=True)
        pdf.set_text_color(0, 0, 0)
        draw_table_header(has_amount)

        pdf.set_font("Helvetica", "", 9)
        account_total = 0.0
        for row in rows:
            if has_amount:
                line_key = str(row.get("source_idx") if row.get("source_idx") is not None else "")
                amount = _resolve_line_amount(pricing_mode, fixed_price, line_prices, line_key)
                account_total += amount
                pdf.cell(82, 6, _pdf_safe(row.get("user_ref_raw", ""))[:44], border=1)
                pdf.cell(70, 6, _pdf_safe(row.get("boe", ""))[:30], border=1)
                pdf.cell(50, 6, _pdf_safe(row.get("imp_code", ""))[:25], border=1)
                pdf.cell(40, 6, _pdf_safe(row.get("created_by", ""))[:18], border=1)
                pdf.cell(36, 6, _pdf_safe(_format_money(amount, currency_code)), border=1, ln=True, align="R")
            else:
                pdf.cell(95, 6, _pdf_safe(row.get("user_ref_raw", ""))[:52], border=1)
                pdf.cell(85, 6, _pdf_safe(row.get("boe", ""))[:38], border=1)
                pdf.cell(55, 6, _pdf_safe(row.get("imp_code", ""))[:28], border=1)
                pdf.cell(42, 6, _pdf_safe(row.get("created_by", ""))[:22], border=1, ln=True)

        if has_amount:
            converted_ghs = _convert_total_to_ghs(account_total, currency_code, manual_rate)
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(0, 7, _pdf_safe(f"Account Amount Total: {_format_money(account_total, currency_code)}"), ln=True, align="R")
            pdf.set_font("Helvetica", "", 8)
            pdf.cell(0, 6, _pdf_safe(f"Amount in words: {_amount_in_words(account_total, currency_code)}"), ln=True, align="R")
            if converted_ghs is not None:
                pdf.cell(0, 6, _pdf_safe(f"Converted Total (GHS): {_format_money(converted_ghs, 'GHS')}"), ln=True, align="R")
                pdf.cell(0, 6, _pdf_safe(f"Converted Amount in words (GHS): {_amount_in_words(converted_ghs, 'GHS')}"), ln=True, align="R")
            if conversion_note:
                pdf.cell(0, 6, _pdf_safe(conversion_note), ln=True, align="R")

        pdf.ln(3)

    pdf_content = pdf.output(dest="S")
    if isinstance(pdf_content, str):
        pdf_content = pdf_content.encode("latin-1", errors="ignore")
    return BytesIO(pdf_content)


def build_all_accounts_view_html(account_report, generated_at_text, company_profile=None, pricing_by_account=None):
    accounts = account_report.get("accounts", [])
    pricing_by_account = pricing_by_account or {}
    profile = company_profile or {}
    company_name = str(profile.get("name") or "").strip()
    company_address = str(profile.get("address") or "").strip()
    company_phone = str(profile.get("phone") or "").strip()
    company_logo_path = str(profile.get("logo_path") or "").strip()
    company_logo_html = f'<img src="/{company_logo_path}" alt="Company Logo" style="max-height:72px;max-width:180px;display:block;margin-bottom:8px;">' if company_logo_path else ""
    company_block = ""
    if company_name or company_address or company_phone or company_logo_html:
        company_block = f"""
        <div class="company-header">
            {company_logo_html}
            <h2>{company_name or ''}</h2>
            <p>{company_address or ''}</p>
            <p>{('Tel: ' + company_phone) if company_phone else ''}</p>
        </div>
        """

    html = f"""
    <html>
    <head>
        <title>All Account Groups</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
            .company-header {{ background: #ffffff; border: 1px solid #dbe4f2; border-radius: 8px; padding: 12px; margin-bottom: 14px; }}
            .company-header h2 {{ margin: 0 0 6px 0; color: #17467d; }}
            .company-header p {{ margin: 3px 0; color: #2d3b4f; }}
            .header {{ background: #17467d; color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
            .header h1 {{ margin: 0 0 5px 0; }}
            .header p {{ margin: 5px 0; font-size: 0.9rem; }}
            .group-card {{ background: #fff; border: 1px solid #dbe4f2; border-radius: 10px; margin-bottom: 16px; overflow: hidden; }}
            .group-head {{ background: #e8ecf1; color: #17467d; font-weight: 700; padding: 10px 12px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; }}
            th {{ background: #f2f5fa; padding: 10px 12px; text-align: left; font-weight: bold; border-bottom: 1px solid #d2dbe8; }}
            td {{ padding: 9px 12px; border-bottom: 1px solid #e8edf5; }}
            tr:hover {{ background: #f9fbff; }}
            .back-link {{ margin-bottom: 20px; }}
            .back-link a {{ color: #17467d; text-decoration: none; font-weight: bold; }}
            .back-link a:hover {{ text-decoration: underline; }}
        </style>
    </head>
    <body>
        <div class="back-link">
            <a href="javascript:history.back()">← Back to Report</a>
        </div>
        {company_block}
        <div class="header">
            <h1>All Account Groups</h1>
            <p>Total Accounts: {len(accounts)}</p>
            <p>Generated: {generated_at_text}</p>
        </div>
    """

    for account in accounts:
        rows = account.get("rows", [])
        html += f"""
        <div class="group-card">
            <div class="group-head">{account.get('name', '')} ({len(rows)} rows)</div>
            <table>
                <thead>
                    <tr>
                        <th>User Ref</th>
                        <th>BOE Number</th>
                        <th>IMP/EXP Code</th>
                        <th>Created By</th>
                        <th>Submission Date</th>
                        <th>Amount</th>
                    </tr>
                </thead>
                <tbody>
        """
        pricing_mode, fixed_price, line_prices, currency_code, manual_rate, conversion_note = _normalize_pricing_profile(pricing_by_account.get(str(account.get("name", ""))))
        account_total = 0.0
        for row in rows:
            line_key = str(row.get("source_idx") if row.get("source_idx") is not None else "")
            amount = _resolve_line_amount(pricing_mode, fixed_price, line_prices, line_key)
            account_total += amount
            html += f"""
                    <tr>
                        <td>{row.get('user_ref_raw', '')}</td>
                        <td>{row.get('boe', '')}</td>
                        <td>{row.get('imp_code', '')}</td>
                        <td>{row.get('created_by', '')}</td>
                        <td>{row.get('date', '')}</td>
                        <td style="text-align:right;">{_format_money(amount, currency_code)}</td>
                    </tr>
            """
        converted_ghs = _convert_total_to_ghs(account_total, currency_code, manual_rate)

        html += """
                </tbody>
                <tfoot>
                    <tr>
                        <td colspan="5" style="font-weight:700;text-align:right;">Account Amount Total</td>
                        <td style="text-align:right;font-weight:700;">""" + f"{_format_money(account_total, currency_code)}" + """</td>
                    </tr>
                    <tr>
                        <td colspan="6" style="font-size:0.9rem;color:#1b3555;"><strong>Amount in words:</strong> """ + f"{_amount_in_words(account_total, currency_code)}" + """</td>
                    </tr>
                    """ + (
                        "<tr><td colspan=\"5\" style=\"font-weight:700;text-align:right;\">Converted Total (GHS)</td>"
                        f"<td style=\"text-align:right;font-weight:700;\">{_format_money(converted_ghs, 'GHS')}</td></tr>"
                        f"<tr><td colspan=\"6\" style=\"font-size:0.9rem;color:#1b3555;\"><strong>Converted Amount in words (GHS):</strong> {_amount_in_words(converted_ghs, 'GHS')}</td></tr>"
                        if converted_ghs is not None else ""
                    ) + """
                </tfoot>
            </table>
            """ + (f"<div style=\"padding:4px 12px 10px;color:#2d3b4f;\"><strong>Conversion:</strong> {conversion_note}</div>" if conversion_note else "") + """
        </div>
        """

    html += """
    </body>
    </html>
    """
    return html
