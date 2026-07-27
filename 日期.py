import pandas as pd
from datetime import date, timedelta

# ==================== 官方节假日及调休规则 ====================
holidays_rules = {
    2024: [
        ("元旦", date(2024, 1, 1), date(2024, 1, 1), []),  # 与周末连休，实际共3天
        ("春节", date(2024, 2, 10), date(2024, 2, 17), [date(2024, 2, 4), date(2024, 2, 18)]),
        ("清明节", date(2024, 4, 4), date(2024, 4, 6), [date(2024, 4, 7)]),
        ("劳动节", date(2024, 5, 1), date(2024, 5, 5), [date(2024, 4, 28), date(2024, 5, 11)]),
        ("端午节", date(2024, 6, 8), date(2024, 6, 10), []),
        ("中秋节", date(2024, 9, 15), date(2024, 9, 17), [date(2024, 9, 14)]),
        ("国庆节", date(2024, 10, 1), date(2024, 10, 7), [date(2024, 9, 29), date(2024, 10, 12)]),
    ],
    2025: [
        ("元旦", date(2025, 1, 1), date(2025, 1, 1), []),   # 周三，不调休，1天
        ("春节", date(2025, 1, 28), date(2025, 2, 4), [date(2025, 1, 26), date(2025, 2, 8)]),
        ("清明节", date(2025, 4, 4), date(2025, 4, 6), []),
        ("劳动节", date(2025, 5, 1), date(2025, 5, 5), [date(2025, 4, 27)]),   # 官方只有4月27日上班
        ("端午节", date(2025, 5, 31), date(2025, 6, 2), []),
        ("国庆节+中秋节", date(2025, 10, 1), date(2025, 10, 8), [date(2025, 9, 28), date(2025, 10, 11)]),
    ],
    2026: [
        ("元旦", date(2026, 1, 1), date(2026, 1, 3), [date(2026, 1, 4)]),  # ✅ 已修正为3天
        ("春节", date(2026, 2, 15), date(2026, 2, 23), [date(2026, 2, 14), date(2026, 2, 28)]),
        ("清明节", date(2026, 4, 4), date(2026, 4, 6), []),
        ("劳动节", date(2026, 5, 1), date(2026, 5, 5), [date(2026, 5, 9)]),
        ("端午节", date(2026, 6, 19), date(2026, 6, 21), []),
        ("中秋节", date(2026, 9, 25), date(2026, 9, 27), []),
        ("国庆节", date(2026, 10, 1), date(2026, 10, 7), [date(2026, 9, 20), date(2026, 10, 10)]),
    ],
}

def get_holiday_info(dt):
    year = dt.year
    if year not in holidays_rules:
        return False, None
    for name, start, end, _ in holidays_rules[year]:
        if start <= dt <= end:
            return True, name
    return False, None

def is_workday_for_holiday(dt):
    year = dt.year
    if year not in holidays_rules:
        return False
    for _, _, _, work_days in holidays_rules[year]:
        if dt in work_days:
            return True
    return False

# 生成日期序列
start_date = date(2024, 1, 1)
end_date = date(2026, 12, 31)
dates = []
current = start_date
while current <= end_date:
    dates.append(current)
    current += timedelta(days=1)

# 构建DataFrame
rows = []
for dt in dates:
    year = dt.year
    month = dt.month
    day = dt.day
    week_day = dt.isoweekday()  # 周一=1, 周日=7
    week_day_name = ["", "周一", "周二", "周三", "周四", "周五", "周六", "周日"][week_day]
    is_weekend = week_day in (6, 7)
    is_holiday, holiday_name = get_holiday_info(dt)
    is_workday_holiday = is_workday_for_holiday(dt)
    
    if is_holiday:
        date_type = "holiday"
    elif is_weekend and is_workday_holiday:
        date_type = "workday"
    elif is_weekend:
        date_type = "weekend"
    else:
        date_type = "workday"
    
    rows.append({
        "date": dt.strftime("%Y-%m-%d"),
        "year": year,
        "month": month,
        "day": day,
        "week_day": week_day,
        "week_day_name": week_day_name,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "holiday_name": holiday_name if holiday_name else "",
        "is_workday_for_holiday": is_workday_holiday,
        "date_type": date_type,
        "year_month": f"{year}-{month:02d}",
    })

df = pd.DataFrame(rows)
print(f"✅ 生成完成，共 {len(df)} 行")
print("\n📅 2026年元旦假期预览：")
print(df[(df['year']==2026) & (df['holiday_name']=='元旦')][['date', 'week_day_name', 'date_type']])
print(df[(df['year']==2026)])
df.to_csv("日期维度表.csv", index=False, encoding="utf-8-sig")
