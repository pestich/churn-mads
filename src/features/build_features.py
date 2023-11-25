import pandas as pd
import random
from tqdm.notebook import tqdm
tqdm.pandas()


class FeatureBuilder:

    def __init__(self) -> None:
        self.sauces = ['соус', 'кетчуп']
        self.main_food = ['бургер', 'воппер', 'ролл', 'ангус', 'гранд чиз', 'цезарь', 'стейкхаус', 'чикен', 
                          'кинг букет', 'бекон кинг', 'острый инди', 'итальяно кинг', 'чеддер бекон', 'а4', 'инди пармезан', 
                          'острый чеддер', 'беконайзер', 'начос кинг', 'биг кинг', 'льзитер', 'конверт']
        self.snacks = ['фри', 'крылышки', 'креветки', 'луковые', 'кольца', 'наггетс', 'стрипс', 'картофель', 
                       'медальоны', 'карт.', 'кинг гоу', 'деревенский', 'начос бокс', 'попкорн', 'сухарики', 'чипсы']
        self.cold_drinks = ['мандарин', 'фрустайл ', 'пепси', 'миринда', 'дюшес', 'сок', 'кола', 'липтон', 
                            'эвервесс', 'байкал', 'лимонад', 'севен ап', 'вода', 'flash up', 'адреналин', ]
        self.hot_drinks = ['кофе','эспрессо','латте','капучино','чай','какао']
        self.alco = ['пиво', 'балтика']
        self.desert = ['пирожок', 'рожок', 'улитка', 'сандэй', 'маффин', 'брауни', 'мороженое','айс ист', 'шейк',]
        self.outlet_churn_rate = pd.read_csv('./data/outlet_churn_rate.csv')
        self.outlet_churn_rate = self.outlet_churn_rate.groupby(['ownareaall_sqm', 'format_name']).agg({'churn_rate': 'mean'}).sort_values(by='churn_rate')
        self.outlet_churn_rate.columns = ['churn_rate']
    
    def ownareaall_category(self, ownareaall_sqm: int, low: int, medium: int) -> int:
        """
        Категоризация площади в зависимости от значения значения.

        Parameters:
        - ownareaall_sqm (int): Значение площади.
        - low (int): Нижняя граница для категории 0.
        - medium (int): Нижняя граница для категории 1.

        Returns:
        - int: Категория (0, 1 или 2).
        """
        if ownareaall_sqm <= low:
            return 0
        if ownareaall_sqm <= medium:
            return 1
        return 2
    

    def get_churn_rate(self, row: int) -> float:
        try:
            return self.outlet_churn_rate.iloc[row['outlet_id']]
        except:
            return 0.8
        

    def outlet_id(self, row):
        try:
            return self.outlet_churn_rate.index.get_loc((row['ownareaall_sqm'], row['format_name']))
        except:
            return -1
        
    def ownareaall_category(self, ownareaall_sqm, low, medium):
        if ownareaall_sqm <= low:
            return 0
        if ownareaall_sqm <= medium:
            return 1
        return 2
    
    
    def prepare_data(self, df: pd.DataFrame, save_to_disk=False) -> pd.DataFrame:
        df['dish_name'] = df['dish_name'].str.lower()
        df['sauces'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.sauces) else 0)
        df['main_food'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.main_food) else 0)
        df['snacks'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.snacks) else 0)
        df['cold_drinks'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.cold_drinks) else 0)
        df['hot_drinks'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.hot_drinks) else 0)
        df['alco'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.alco) else 0)
        df['desert'] = df['dish_name'].progress_apply(lambda text: 1 if any(sub in text for sub in self.desert) else 0)
        data_food_cat = df.groupby(['customer_id','startdatetime'])[['sauces', 'main_food', 'snacks', 'cold_drinks', 'hot_drinks', 'alco', 'desert']].sum()
        data_food_cat = data_food_cat.groupby('customer_id')[['sauces', 'main_food', 'snacks', 'cold_drinks', 'hot_drinks', 'alco', 'desert']].mean()
 

        data_pivot = pd.pivot_table(df, values=['format_name','ownareaall_sqm', 'revenue'], index=['customer_id','startdatetime'],
                        aggfunc={'format_name': 'last', 'ownareaall_sqm': 'last', 'revenue': 'sum'})
        data_pivot = data_pivot.reset_index()
        low = data_pivot.ownareaall_sqm.quantile(0.33).round()
        medium = data_pivot.ownareaall_sqm.quantile(0.66).round()
        data_pivot['ownareaall_cat'] = data_pivot['ownareaall_sqm'].apply(lambda x: self.ownareaall_category(x, low, medium))
        print('ownareaall_cat done')

        data_pivot['weekday'] = data_pivot['startdatetime'].dt.weekday
        data_pivot['time_of_day'] = (data_pivot['startdatetime'].dt.hour + 1) // 6 
        data_pivot['time_of_day'] = data_pivot['time_of_day'].replace(4,0)
        print('time_of_day done')

        data_pivot['outlet_id'] = data_pivot[['ownareaall_sqm', 'format_name']].progress_apply(self.outlet_id, axis=1)
        print('outlet_id done')

        data_pivot['churn_rate'] = data_pivot[['outlet_id']].progress_apply(self.get_churn_rate, axis=1)
        print('churn_rate done')

        cols_num = 3
        for i in range(1, cols_num + 1):
            data_pivot[f'{i}InvoiceDate'] = data_pivot.groupby('customer_id')['startdatetime'].shift(i)
        for i in range(1, cols_num + 1):
            data_pivot[f'{i}DayDiff'] = (data_pivot['startdatetime'] - data_pivot[f'{i}InvoiceDate']).dt.seconds
        data_day_diff = data_pivot.groupby('customer_id').agg({'1DayDiff': ['mean','std']}).reset_index()
        data_day_diff.columns = data_day_diff.columns.get_level_values(0)
        data_day_order_last = data_pivot.drop_duplicates(subset=['customer_id'],keep='last')
        data_day_order_last = data_day_order_last[['customer_id', '1DayDiff', '2DayDiff']]
        data_day_order_last = data_day_order_last.merge(data_day_diff, on='customer_id')
        data_day_order_last.columns = ['customer_id', 'day_diff_1', 'day_diff_2', 'day_diff_mean', 'day_diff_std']
        print('data_day_order_last done')

        data_area = data_pivot.groupby('customer_id').agg({'ownareaall_sqm': 'mean'}).reset_index()
        data_area.columns = ['customer_id','ownareaall_sqm_mean']
        print('data_area done')

        data_churn_rate = data_pivot.groupby('customer_id').agg({'churn_rate': ['mean','std'],
                                                            'outlet_id': lambda x: random.choice(pd.Series.mode(x))}).reset_index()
        data_churn_rate.columns = ['customer_id', 'churn_rate_mean', 'churn_rate_std', 'favourite_outlet_id']
        data_churn_rate['favourite_churn_rate'] = data_churn_rate['favourite_outlet_id'].apply(lambda x: self.outlet_churn_rate.iloc[x])
        print('data_churn_rate done')

        purchase_counts = data_pivot.groupby('customer_id').size()
        total_revenue_per_customer = data_pivot.groupby('customer_id')['revenue'].sum()
        average_revenue_per_customer = data_pivot.groupby('customer_id')['revenue'].mean()
        data_revenue = pd.DataFrame({
            'total_revenue': total_revenue_per_customer,
            'average_revenue': average_revenue_per_customer,
            'count': purchase_counts
        })
        data_revenue = data_revenue.reset_index()
        print('data_revenue done')


        data_full = (data_revenue.merge(data_food_cat, on = 'customer_id')
                .merge(data_area, on = 'customer_id')
                .merge(data_churn_rate, on = 'customer_id')
                .merge(data_day_order_last, on = 'customer_id'))
        
        if save_to_disk:
            data_full.to_csv('./data_submit_full.csv')
            
        return data_full
