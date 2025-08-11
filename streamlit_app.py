import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# try to import autorefresh lib; fallback to no-autorefresh if not installed
try:
    from streamlit_autorefresh import st_autorefresh
    HAVE_AUTOREFRESH = True
except Exception:
    HAVE_AUTOREFRESH = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_URL = os.getenv('DB_URL', f"sqlite:///{os.path.join(BASE_DIR, 'orders.db')}")
engine = create_engine(DB_URL, connect_args={'check_same_thread': False} if DB_URL.startswith('sqlite') else {})

st.set_page_config(page_title='E-commerce Realtime Dashboard', layout='wide')
st.title('📦 Realtime Dashboard')

if HAVE_AUTOREFRESH:
    st_autorefresh(interval=3000, key='datarefresh')

@st.cache_data(ttl=2)
def load_data():
    try:
        df = pd.read_sql('SELECT * FROM orders ORDER BY order_timestamp DESC', con=engine)
        if not df.empty:
            df['order_timestamp'] = pd.to_datetime(df['order_timestamp'])
        return df
    except Exception as e:
        st.error('DB read error: ' + str(e))
        return pd.DataFrame()

col1, col2 = st.columns([3, 1])
with col2:
    st.header('Filters')
    min_amount = st.number_input('Min total amount', min_value=0, value=0)
    location_filter = st.selectbox('Location', options=['All', 'Delhi', 'Mumbai', 'Bengaluru', 'Kolkata', 'Chennai'], index=0)

df = load_data()
if df.empty:
    st.info('No orders yet. Producer और Processor चलाएँ।')
else:
    if min_amount > 0:
        df = df[df['total_amount'] >= min_amount]
    if location_filter != 'All':
        df = df[df['location'] == location_filter]

    total_sales = df['total_amount'].sum()
    total_orders = len(df)
    avg_order = df['total_amount'].mean()

    st.metric('Total Sales', f'₹ {total_sales:,.2f}')
    st.metric('Total Orders', total_orders)
    st.metric('Average Order Value', f'₹ {avg_order:,.2f}' if not pd.isna(avg_order) else '₹ 0.00')

    st.subheader('Orders (top 50)')

    def mark_received(oid):
        try:
            with engine.begin() as conn:
                conn.execute(text("UPDATE orders SET received='yes' WHERE order_id = :oid"), {'oid': oid})
        except Exception as e:
            st.error('Update failed: ' + str(e))

    for _, row in df.head(50).iterrows():
        c = st.columns([1, 3, 1, 1, 1, 1, 1])
        c[0].write(row['order_id'])
        c[1].write(row['product_name'])
        c[2].write(int(row['quantity']))
        c[3].write(f"₹ {row['total_amount']:,.2f}")
        c[4].write(row['location'])
        c[5].write(row['order_timestamp'])
        if row.get('received') == 'yes':
            c[6].success('Received')
        else:
            if c[6].button('Mark Received', key=f"r_{row['order_id']}"):
                mark_received(row['order_id'])
                try:
                    st.rerun()
                except Exception:
                    pass

    st.subheader('Sales by Product')
    prod = df.groupby('product_name')['total_amount'].sum().reset_index().sort_values('total_amount', ascending=False)
    st.bar_chart(prod.rename(columns={'product_name': 'index'}).set_index('index'))
