#读取苹果股价数据
import pandas as pd

df = pd.read_excel("AAPL.O.xlsx", engine='openpyxl')
print(f"✅ 成功读取数据，原始数据形状：{df.shape}（行×列）")
print(f"📅 原始数据列名：{list(df.columns)}")


date_column = '日期'     # 数据预处理：转换日期格式并设置为索引
df[date_column] = pd.to_datetime(df[date_column], errors='coerce')    # 转换日期格式为datetime类型
df = df.dropna(subset=[date_column])
print(f"🗑️ 删除无效日期后，数据形状：{df.shape}")   # 删除日期为空或无效的行


df = df.set_index(date_column)   # 将日期列设为索引（便于时间维度操作）
df = df.sort_index()    # 按日期升序排序

#转换为工作日并处理缺失值
df = df.copy()
workday_resampled = df.resample('B').asfreq()   
rows_with_missing = workday_resampled[workday_resampled.isnull().any(axis=1)].copy()   
print(f"✅ 共找到 {len(rows_with_missing)} 行含缺失值：")
#检查发现这些工作日因为一些特定的节日美国休市因此直接删除
APPL = workday_resampled.dropna(how='any')
print(f"🗑️ 删除节日日期后，数据形状：{APPL.shape}")

APPL = APPL.drop(columns=["名称","成交额(百万)"])  
print("✅ 已删除“名称”列")

column_mapping = {
    '代码': 'Code',
    '开盘价(元)': 'Open($)',
    '最高价(元)': 'High($)',
    '最低价(元)': 'Low($)',
    '收盘价(元)': 'Close($)',
    '涨跌幅': 'Change',
    '成交量(股)': 'Volume(Shares)'
}
APPL = APPL.rename(columns=column_mapping)
APPL.index.name = 'Date'
# 筛选2020-10-01 到 现在 的数据
start_date = '2020-10-01'
APPL = APPL.loc[start_date:]

#数据清洗
# 1. 缺失值检查与处理 
# 1.1 统计各列缺失值数量和比例
missing_info = pd.DataFrame({
    '缺失值数量': APPL.isnull().sum(),
    '缺失值占比(%)': round(APPL.isnull().sum() / len(APPL) * 100, 4)
})
print("\n各列缺失值统计：")
print(missing_info)

# 1.2 缺失值处理（时间序列前向+后向填充）
total_missing = APPL.isnull().sum().sum()
if total_missing > 0:
    numeric_cols = ['Open($)', 'High($)', 'Low($)', 'Close($)', 'Change', 'Volume(Shares)']
    APPL[numeric_cols] = APPL[numeric_cols].fillna(method='ffill')  # 前向填充（延续前一日数据）
    APPL = APPL.fillna(method='bfill')  # 剩余缺失值后向填充兜底
    print(f"\n✅ 处理方式：数值列采用前向填充+后向填充")
    print(f"✅ 处理结果：缺失值总数从 {total_missing} 降至 {APPL.isnull().sum().sum()}")
else:
    print(f"\n✅ 无缺失值（总计缺失值：{total_missing} 个），无需处理")

# 2. 异常值检测与处理（含涨跌幅异常）
# 2.1 定义核心列
price_cols = ['Open($)', 'High($)', 'Low($)', 'Close($)']  # 价格列
change_col = 'Change'  # 涨跌幅列
volume_col = 'Volume(Shares)'  # 成交量列
all_numeric_cols = price_cols + [change_col, volume_col]

# 2.2 价格负值异常检验与处理（核心要求）
price_negative_count = {}
total_price_negative = 0
for col in price_cols:
    neg_count = (APPL[col] < 0).sum()
    price_negative_count[col] = neg_count
    total_price_negative += neg_count
    
    if neg_count > 0:
        # 处理策略：价格负值替换为该列非负数据的均值
        mean_val = APPL[col][APPL[col] >= 0].mean()
        APPL.loc[APPL[col] < 0, col] = mean_val
        print(f"{col}：")
        print(f"  - 负值异常数量：{neg_count} 个")
        print(f"  - 处理方式：替换为该列非负数据均值（{round(mean_val, 4)}）")
    else:
        print(f"{col}：无负值异常（0 个）")

# 2.3 涨跌幅极端值异常检验（美股无涨跌幅限制，但±20%以上视为数据错误）
change_threshold = 20  # 自定义阈值：±20%
APPL[change_col] = pd.to_numeric(APPL[change_col], errors='coerce')
# 统计极端涨跌幅数量
extreme_change_mask = (APPL[change_col] > change_threshold) | (APPL[change_col] < -change_threshold)
extreme_change_count = extreme_change_mask.sum()

if extreme_change_count > 0:
    # 处理策略：极端涨跌幅替换为该列正常范围（-20%~20%）的中位数
    normal_change = APPL[change_col][~extreme_change_mask]
    median_val = normal_change.median()
    APPL.loc[extreme_change_mask, change_col] = median_val
    print(f"\n{change_col}（涨跌幅）：")
    print(f"  - 极端值阈值：±{change_threshold}%")
    print(f"  - 极端值异常数量：{extreme_change_count} 个")
    print(f"  - 处理方式：替换为正常范围数据中位数（{round(median_val, 4)}%）")
else:
    print(f"\n{change_col}（涨跌幅）：无极端值异常（0 个，阈值±{change_threshold}%）")

# 2.4 股票价格逻辑校验（最高价≥开/收盘价≥最低价）
price_logic_error = (APPL['High($)'] < APPL['Close($)']) | \
                    (APPL['High($)'] < APPL['Open($)']) | \
                    (APPL['Low($)'] > APPL['Close($)']) | \
                    (APPL['Low($)'] > APPL['Open($)'])
logic_error_count = price_logic_error.sum()

if logic_error_count > 0:
    # 修正策略：最高价取该行开/收/高的最大值，最低价取开/收/低的最小值
    APPL.loc[price_logic_error, 'High($)'] = APPL.loc[price_logic_error, ['Open($)', 'Close($)', 'High($)']].max(axis=1)
    APPL.loc[price_logic_error, 'Low($)'] = APPL.loc[price_logic_error, ['Open($)', 'Close($)', 'Low($)']].min(axis=1)
    print(f"\n价格逻辑校验：")
    print(f"  - 异常数量：{logic_error_count} 行（最高价<开/收盘价 或 最低价>开/收盘价）")
    print(f"  - 处理方式：修正最高价/最低价为合理范围")
else:
    print(f"\n价格逻辑校验：无异常（{logic_error_count} 行错误）")

# 2.5 成交量非负校验
volume_negative = (APPL[volume_col] < 0).sum()
if volume_negative > 0:
    APPL.loc[APPL[volume_col] < 0, volume_col] = 0
    print(f"{volume_col}（成交量）：")
    print(f"  - 负值异常数量：{volume_negative} 行")
    print(f"  - 处理方式：修正为0")
else:
    print(f"{volume_col}（成交量）：无负值异常（0 行）")

# 2.6 异常值处理总计
total_abnormal = total_price_negative + extreme_change_count + logic_error_count + volume_negative
print(f"\n✅ 异常值处理总计：共处理 {total_abnormal} 个异常点")
print(f"   - 价格负值异常：{total_price_negative} 个")
print(f"   - 涨跌幅极端值异常：{extreme_change_count} 个")
print(f"   - 价格逻辑异常：{logic_error_count} 个")
print(f"   - 成交量负值异常：{volume_negative} 个")

# 3. 数据格式最终校验
# 3.1 日期格式校验
if isinstance(APPL.index, pd.DatetimeIndex) and pd.api.types.is_datetime64_any_dtype(APPL.index):
    print(f"日期格式：✅ 索引为DatetimeIndex，格式正确（{APPL.index.dtype}）")
else:
    APPL.index = pd.to_datetime(APPL.index)
    print(f"日期格式：⚠️  原格式错误，已转换为DatetimeIndex（{APPL.index.dtype}）")

# 3.2 数值格式校验
format_error_cols = []
for col in all_numeric_cols:
    if not pd.api.types.is_numeric_dtype(APPL[col]):
        format_error_cols.append(col)
        APPL[col] = pd.to_numeric(APPL[col], errors='coerce')

if format_error_cols:
    print(f"数值格式：⚠️  以下列非数值型，已转换：{format_error_cols}")
else:
    print(f"数值格式：✅ 所有数值列格式正确（{all_numeric_cols}）")

# 4. 清洗结果总结
print("\n" + "="*60)
print("📌 数据清洗最终总结")
print("="*60)
print(f"清洗后数据行数：{len(APPL)}（无行删除，仅异常值修正）")
print(f"最终缺失值总数：{APPL.isnull().sum().sum()} 个")
print(f"处理的异常类型及数量：")
print(f"  • 价格负值：{total_price_negative} 个")
print(f"  • 涨跌幅极端值（±{change_threshold}%）：{extreme_change_count} 个")
print(f"  • 价格逻辑错误：{logic_error_count} 个")
print(f"  • 成交量负值：{volume_negative} 个")
print(f"数据时间范围：{APPL.index.min()} ~ {APPL.index.max()}")
print(f"✅ 数据清洗完成，可用于后续分析")