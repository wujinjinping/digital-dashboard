# app.py
import os, io, base64, pandas as pd, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from flask import Flask, request, render_template_string
import duckdb

# ===== 使用本地宋体 =====
import os, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
font_path = os.path.join(os.path.dirname(__file__), 'SimSun.ttf')
font_manager.fontManager.addfont(font_path)
plt.rcParams['font.family'] = font_manager.FontProperties(fname=font_path).get_name()
plt.rcParams['axes.unicode_minus'] = False
# ========================
app = Flask(__name__)
DB_FILE   = 'digi.db'
CSV_FILE  = 'data_1999_2023.csv'
EXCEL_IND = '最终数据dta格式-上市公司年度行业代码至2021.xlsx'

# ---------- 1. 建库：指数 + 行业（补全2022-2023） ----------
def init_db():
    if os.path.exists(DB_FILE):
        return
    print('首次运行：合并行业+指数，补全2022-2023...')
    df_main = pd.read_csv(CSV_FILE, on_bad_lines='skip')
    df_main.columns = df_main.columns.str.strip()
    df_ind = pd.read_excel(EXCEL_IND, dtype=str)
    df_ind.columns = df_ind.columns.str.strip()
    df_ind = df_ind.rename(columns={
        '股票代码全称': '股票代码',
        '年度': '年份',
        '行业代码': '行业代码',
        '行业名称': '行业名称'
    })[['股票代码', '年份', '行业代码', '行业名称']]
    max_year = int(df_ind['年份'].max())
    last_year = df_ind[df_ind['年份'] == str(max_year)]
    for y in range(max_year + 1, 2024):
        tmp = last_year.copy()
        tmp['年份'] = str(y)
        df_ind = pd.concat([df_ind, tmp], ignore_index=True)
    df = df_main.merge(df_ind, on=['股票代码', '年份'], how='left')
    df['行业代码'] = df['行业代码'].fillna('—')
    df['行业名称'] = df['行业名称'].fillna('暂未分类')
    df['年份'] = pd.to_numeric(df['年份'].str.strip(), errors='coerce').fillna(0).astype(int)

    col_comp, col_norm = df.columns[-4], df.columns[-3]
    df[col_comp] = pd.to_numeric(df[col_comp], errors='coerce')
    df[col_norm] = pd.to_numeric(df[col_norm], errors='coerce')

    con = duckdb.connect(DB_FILE)
    con.register('df_view', df)
    con.execute('CREATE TABLE digi AS SELECT * FROM df_view')
    con.execute('CREATE INDEX idx_code ON digi("股票代码")')
    con.execute('CREATE INDEX idx_year ON digi("年份")')
    con.close()
    print('DuckDB 初始化完成！')

init_db()

# ---------- 2. 通用查询 ----------
def query_duck(code=None, name=None, year=None, ind_code=None, ind_name=None):
    con = duckdb.connect(DB_FILE)
    sql = 'SELECT * FROM digi WHERE 1=1'
    args = {}
    if code:
        sql += ' AND "股票代码" = $code'
        args['code'] = code
    if year:
        sql += ' AND "年份" = $year'
        args['year'] = int(year)
    if name:
        sql += ' AND "企业名称" LIKE $name'
        args['name'] = f'%{name}%'
    if ind_code:
        sql += ' AND UPPER("行业代码") = UPPER($ind_code)'
        args['ind_code'] = ind_code
    if ind_name:
        sql += ' AND "行业名称" LIKE $ind_name'
        args['ind_name'] = f'%{ind_name}%'
    df = con.execute(sql, args).df()
    con.close()
    return df

# ---------- 3. 首页（四表单） ----------
HOME_HTML = '''
<!doctype html>
<title>数字化转型仪表盘</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.staticfile.org/bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#f8f9fa}.card{margin:20px 0}.index-big{font-size:2.2rem;font-weight:700}</style>
<div class="container">
  <h2 class="text-center mt-4">上市公司数字化转型指数查询仪表盘</h2>

<div class="alert alert-info alert-dismissible fade show mt-3" role="alert">
  <strong>使用提示：</strong>
  ① 输入股票代码或企业名称关键字即可查询；
  ② 选择行业代码/名称可查看行业整体趋势；
  ③ 支持双企业对比；
  ④ 可生成指定年份的行业折线图对比。
  <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
</div>


  <!-- ① 企业查询 -->
  <div class="card shadow">
    <div class="card-body">
      <h5 class="card-title">① 选择企业</h5>
      <form action="/query" method="get">
        <div class="row g-3">
          <div class="col-md-4"><input class="form-control" name="code" placeholder="股票代码 如 600000"></div>
          <div class="col-md-4"><input class="form-control" name="name" placeholder="企业名称关键字 如 万科"></div>
          <div class="col-md-2"><input class="form-control" name="year" placeholder="年份(选) 如 2023"></div>
          <div class="col-md-2"><button class="btn btn-primary w-100">查询</button></div>
        </div>
      </form>
    </div>
  </div>

  <!-- ② 行业反查 -->
  <div class="card shadow">
    <div class="card-body">
      <h5 class="card-title">② 按行业查询</h5>
      <form action="/industry" method="get">
        <div class="row g-3">
          <div class="col-md-4"><input class="form-control" name="ind_code" placeholder="行业代码 如 C27"></div>
          <div class="col-md-4"><input class="form-control" name="ind_name" placeholder="行业名称关键字 如 医药"></div>
          <div class="col-md-2"><input class="form-control" name="year" placeholder="年份(选) 如 2020"></div>
          <div class="col-md-2"><button class="btn btn-success w-100">查询行业</button></div>
        </div>
      </form>
    </div>
  </div>

  <!-- ③ 双企业对比 -->
  <div class="card shadow">
    <div class="card-body">
      <h5 class="card-title">③ 双企业对比</h5>
      <form action="/compare" method="get">
        <div class="row g-3">
          <div class="col-md-5"><input class="form-control" name="code1" placeholder="主企业 股票代码" required></div>
          <div class="col-md-5"><input class="form-control" name="code2" placeholder="对比企业 股票代码" required></div>
          <div class="col-md-2"><button class="btn btn-warning w-100">对比</button></div>
        </div>
      </form>
    </div>
  </div>

  <!-- ④ 行业折线图对比 -->
  <div class="card shadow">
    <div class="card-body">
      <h5 class="card-title">④ 行业折线图对比</h5>
      <form action="/industry_bar" method="get">
        <div class="row g-3">
          <div class="col-md-5"><input class="form-control" name="code" placeholder="企业股票代码 如 600000" required></div>
          <div class="col-md-5"><input class="form-control" name="year" placeholder="年份 如 2023" required></div>
          <div class="col-md-2"><button class="btn btn-info w-100">生成折线图</button></div>
        </div>
      </form>
    </div>
  </div>
</div>
'''

@app.route('/')
def home():
    return render_template_string(HOME_HTML)

# ---------- 4. 企业查询 ----------
@app.route('/query')
def query():
    code = request.args.get('code', '').strip()
    name = request.args.get('name', '').strip()
    year = request.args.get('year', '').strip()
    if not code and not name:
        return '<h5>请输入股票代码或企业名称！</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    df = query_duck(code=code or None, name=name or None)
    if df.empty:
        return '<h5>未找到数据，可能尚未上市。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    col_comp, col_norm = df.columns[-4], df.columns[-3]
    for d in (df,):
        d[col_comp] = pd.to_numeric(d[col_comp], errors='coerce')
        d[col_norm] = pd.to_numeric(d[col_norm], errors='coerce')
    trend = df.groupby('年份')[[col_comp, col_norm]].mean().sort_index()
    highlight_idx = None
    if year:
        try:
            highlight_idx = trend.index.get_loc(int(year))
        except KeyError:
            highlight_idx = None
    fig, ax = plt.subplots(figsize=(10, 3.5))
    ax.plot(trend.index, trend[col_comp], marker='o', label=col_comp, color='steelblue', linewidth=2)
    ax.plot(trend.index, trend[col_norm], marker='s', label=col_norm, color='orange', linewidth=2)
    if highlight_idx is not None:
        ax.scatter(int(year), trend.loc[int(year), col_comp], color='red', marker='*', s=150, zorder=5)
        ax.scatter(int(year), trend.loc[int(year), col_norm], color='red', marker='*', s=150, zorder=5)
    ax.set_title(f"{df.iloc[0]['企业名称']}（{df.iloc[0]['行业名称']}）历年数字化转型双指数" + (f" - {year}年标记" if year else ""), fontsize=12)
    ax.set_xlabel("年份", fontsize=10); ax.set_ylabel("指数", fontsize=10); ax.grid(alpha=0.3)
    ax.set_xticks(trend.index); ax.set_xticklabels(trend.index, rotation=45, ha='right', fontsize=9)
    ax.legend(fontsize=9)
    fig.tight_layout()
    buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=120); buf.seek(0)
    img = base64.b64encode(buf.read()).decode(); plt.close()
    return f'''
    <div class="container">
      <h4 class="mt-4">{df.iloc[0]["企业名称"]} （{df.iloc[0]["股票代码"]}）（所属行业：{df.iloc[0]["行业名称"]}）</h4>
      <img src="data:image/png;base64,{img}" class="img-fluid">
      <div class="mt-3"><a href="/" class="btn btn-secondary">返回首页</a></div>
    </div>
    '''

# ---------- 5. 双企业对比 ----------
@app.route('/compare')
def compare():
    code1 = request.args.get('code1', '').strip()
    code2 = request.args.get('code2', '').strip()
    if not code1 or not code2:
        return '<h5>请填写两家企业股票代码！</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    df1 = query_duck(code=code1)
    df2 = query_duck(code=code2)
    if df1.empty or df2.empty:
        return '<h5>对比企业数据不存在。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    col_comp, col_norm = df1.columns[-4], df1.columns[-3]
    for d in (df1, df2):
        d[col_comp] = pd.to_numeric(d[col_comp], errors='coerce')
        d[col_norm] = pd.to_numeric(d[col_norm], errors='coerce')
    trend1 = df1.groupby('年份')[[col_comp, col_norm]].mean().sort_index()
    trend2 = df2.groupby('年份')[[col_comp, col_norm]].mean().sort_index()
    full_idx = pd.RangeIndex(1999, 2024)
    trend1 = trend1.reindex(full_idx, fill_value=0)
    trend2 = trend2.reindex(full_idx, fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(trend1.index, trend1[col_comp], marker='o', label=f'{code1} {col_comp}')
    ax.plot(trend1.index, trend1[col_norm], marker='s', label=f'{code1} {col_norm}')
    ax.plot(trend2.index, trend2[col_comp], marker='o', linestyle='--', label=f'{code2} {col_comp}')
    ax.plot(trend2.index, trend2[col_norm], marker='s', linestyle='--', label=f'{code2} {col_norm}')
    ax.set_title(f"{code1} vs {code2} 双指数对比")
    ax.set_xlabel("年份"); ax.set_ylabel("指数"); ax.grid(alpha=0.3); ax.legend()
    fig.autofmt_xdate()
    buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=120); buf.seek(0)
    img = base64.b64encode(buf.read()).decode(); plt.close()
    return f'''
    <div class="container">
      <h4 class="mt-4">{code1} vs {code2} 双指数对比</h4>
      <img src="data:image/png;base64,{img}" class="img-fluid">
      <div class="mt-3"><a href="/" class="btn btn-secondary">返回首页</a></div>
    </div>
    '''

# ---------- 6. 行业查询 ----------
@app.route('/industry')
def industry():
    ind_code = request.args.get('ind_code', '').strip()
    ind_name = request.args.get('ind_name', '').strip()
    year = request.args.get('year', '').strip()
    if not ind_code and not ind_name:
        return '<h5>请至少输入行业代码或行业名称关键字！</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    if year:
        df = query_duck(ind_code=ind_code or None, ind_name=ind_name or None, year=int(year))
        if df.empty:
            return '<h5>该年份行业内无数据。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
        col_comp, col_norm = df.columns[-4], df.columns[-3]
        for d in (df,):
            d[col_comp] = pd.to_numeric(d[col_comp], errors='coerce')
            d[col_norm] = pd.to_numeric(d[col_norm], errors='coerce')
        df = df.sort_values(col_comp, ascending=False)
        df20 = df.head(20)
        fig, ax = plt.subplots(figsize=(max(6, len(df20)*0.5), 2.8))
        ax.plot(range(len(df20)), df20[col_comp], marker='o', label=col_comp, color='steelblue', linewidth=2)
        ax.plot(range(len(df20)), df20[col_norm], marker='s', label=col_norm, color='orange', linewidth=2)
        ax.set_title(f"{year}年 {ind_code or ind_name} 行业企业双指数对比（前20）", fontsize=10)
        ax.set_xlabel("企业", fontsize=8); ax.set_ylabel("指数", fontsize=8); ax.grid(alpha=0.3)
        step = max(1, len(df20)//10)
        ax.set_xticks(range(0, len(df20), step))
        ax.set_xticklabels(df20['企业名称'].iloc[::step], rotation=45, ha='right', fontsize=7)
        ax.legend(fontsize=8)
        fig.tight_layout(pad=0.3)
        buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=120); buf.seek(0)
        img = base64.b64encode(buf.read()).decode(); plt.close()
        button_html = f'''
        <form action="/industry_full" method="get" target="_blank">
          <input type="hidden" name="ind_code" value="{ind_code}">
          <input type="hidden" name="ind_name" value="{ind_name}">
          <input type="hidden" name="year" value="{year}">
          <button type="submit" class="btn btn-info mb-3">查看完整列表（新页）</button>
        </form>
        '''
        return f'''
        <div class="container">
          <h4>{year}年 {ind_code or ind_name} 行业企业双指数对比（前20）</h4>
          <img src="data:image/png;base64,{img}" class="img-fluid">
          <h5>前20企业列表</h5>
          <table class="table table-bordered">
            <tr><th>股票代码</th><th>企业名称</th><th>行业代码</th><th>行业名称</th>
                <th>数字化转型综合指数</th><th>标准化转型指数(0-100)</th></tr>
            {''.join(f'<tr><td>{r["股票代码"]}</td><td>{r["企业名称"]}</td><td>{r["行业代码"]}</td>'
                     f'<td>{r["行业名称"]}</td><td>{r[col_comp]:.3f}</td><td>{r[col_norm]:.3f}</td></tr>'
                     for _, r in df20.iterrows())}
          </table>
          {button_html}
          <a href="/" class="btn btn-secondary">返回首页</a>
        </div>
        '''
    # 无年份 → 历年行业平均折线
    df = query_duck(ind_code=ind_code or None, ind_name=ind_name or None)
    if df.empty:
        return '<h5>该行业无数据。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    col_comp, col_norm = df.columns[-4], df.columns[-3]
    df[col_comp] = pd.to_numeric(df[col_comp], errors='coerce')
    df[col_norm] = pd.to_numeric(df[col_norm], errors='coerce')
    trend = df.groupby('年份')[[col_comp, col_norm]].mean().sort_index()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(trend.index, trend[col_comp], marker='o', label=col_comp, color='steelblue')
    ax.plot(trend.index, trend[col_norm], marker='s', label=col_norm, color='orange')
    ax.set_title(f"{ind_code or ind_name} 行业历年数字化转型双指数")
    ax.set_xlabel("年份"); ax.set_ylabel("指数"); ax.grid(alpha=0.3); ax.legend()
    fig.autofmt_xdate()
    buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=120); buf.seek(0)
    img = base64.b64encode(buf.read()).decode(); plt.close()
    return f'''
    <div class="container">
      <h4>{ind_code or ind_name} 行业历年数字化转型双指数</h4>
      <img src="data:image/png;base64,{img}" class="img-fluid">
      <a href="/" class="btn btn-secondary">返回首页</a>
    </div>
    '''

# ---------- 7. 行业折线图对比 ----------
@app.route('/industry_bar')
def industry_bar():
    code = request.args.get('code', '').strip()
    year = request.args.get('year', '').strip()
    if not code or not year:
        return '<h5>请填写企业股票代码和年份！</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    ent = query_duck(code=code)
    if ent.empty:
        return '<h5>该企业不存在。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    ind_name = ent.iloc[0]['行业名称']
    df = query_duck(ind_name=ind_name, year=int(year))
    if df.empty:
        return '<h5>该年份行业内无数据。</h5><a href="/" class="btn btn-sm btn-secondary">返回</a>'
    col_comp, col_norm = df.columns[-4], df.columns[-3]
    df[col_comp] = pd.to_numeric(df[col_comp], errors='coerce')
    df[col_norm] = pd.to_numeric(df[col_norm], errors='coerce')
    df = df.sort_values(col_comp, ascending=False).head(20)
    fig, ax = plt.subplots(figsize=(max(6, len(df)*0.5), 2.8))
    ax.plot(range(len(df)), df[col_comp], marker='o', label=col_comp, color='steelblue', linewidth=2)
    ax.plot(range(len(df)), df[col_norm], marker='s', label=col_norm, color='orange', linewidth=2)
    ax.set_title(f"{year}年 {ind_name} 行业企业双指数对比（前20）", fontsize=10)
    ax.set_xlabel("企业", fontsize=8); ax.set_ylabel("指数", fontsize=8); ax.grid(alpha=0.3)
    step = max(1, len(df)//10)
    ax.set_xticks(range(0, len(df), step))
    ax.set_xticklabels(df['企业名称'].iloc[::step], rotation=45, ha='right', fontsize=7)
    ax.legend(fontsize=8)
    fig.tight_layout(pad=0.3)
    buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=120); buf.seek(0)
    img = base64.b64encode(buf.read()).decode(); plt.close()
    return f'''
    <div class="container">
      <h4 class="mt-4">{year}年 {ind_name} 行业企业双指数对比（前20）</h4>
      <img src="data:image/png;base64,{img}" class="img-fluid">
      <div class="mt-3"><a href="/" class="btn btn-secondary">返回首页</a></div>
    </div>
    '''

# ---------- 8. 启动 ----------
if __name__ == '__main__':
    app.run(debug=True)