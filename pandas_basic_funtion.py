
import pandas as pd



magic_land=pd.read_csv('lands.csv',index_col=0)
objects=(type(magic_land[magic_land.columns[2]].dtypes))




def  data_colunm_get(iter:pd.DataFrame):   #get any dataframe columns for any data
 data_row=[]
 for i in iter:
   data_row.append(i)
 return data_row



def get_dataframe_column(iter:pd.DataFrame,choose_colums:int,end_columns:int|None):  #get any dataframe columns for any data
    if  (choose_colums>len(iter.columns)) or (choose_colums<0):
        raise ValueError("choose_colums is not in range")
    else:
        return iter[iter.columns[choose_colums:end_columns]]



def feature_statistics(data:pd.DataFrame,index:list,feature:str):
     data_feature=data[index]
     for index in data_feature.columns:
      if (type(data_feature[index].dtypes)!=objects) :
       match feature:
         case "mean":
          return data_feature.mean()       #get any dataframe feature for any data
         case "median":                         
          return data_feature.median(axis=1)
         case "max":
             return data_feature.max()
         case "min":
             return data_feature.min()
         case "std":
          return data_feature.std()
         case "var":
          return data_feature.var()
         case 'quantile':
          return data_feature.quantile(q=0.25,axis=1)
         case "count":
          return data_feature.count()
         case "idxmax":
          return data_feature.idxmax()
         case _:
           return("not support feature")
         
      else:raise TypeError("not support dtype is object")
     



def data_uniform(data,index:str):
     print(f"unique data is {repr(data[index].unique())}") 
     print(data[index].value_counts())





def duplicated_data_includ_test(data:pd.DataFrame,index:list,index_another:list):
        if not index or not index_another:
         return False
        elif not all(col in data.columns for col in index):
         return False
        elif not all(col in data.columns for col in index_another):
           return False
        elif not all(col in index for col in index_another):
           return False
        else:
           return True


def duplicated_data(data:pd.DataFrame,index:list,duplicate_index:list):
  if duplicated_data_includ_test(data=data,index=index,index_another=duplicate_index):
    if duplicate_index is not None:
     duplicate_data=data[index]
     return duplicate_data.duplicated(duplicate_index)
    else:return print("empty columns is not valid")
  else:
    raise ValueError("index_another is not in index")
 


def get_muit_columns_data(data:pd.DataFrame,index:list,index_another:list|None,keep:bool|str=False): # dataframe colunms index_another must include at  colunms index,can be call drop_duplicates()
    if index_another is None:
     return data.drop_duplicates(index)
    if index_another is not None:
     if duplicated_data_includ_test(data=data,index=index,index_another=index_another):
      include_data=data[index]   
      return include_data.drop_duplicates(index_another,keep=keep)
     else:
      raise ValueError("index_another is not in index")
      



def replace_data(data:pd.DataFrame,index_columns:list,replace_text:any):
        if type(replace_text)==type({}):
          return data[index_columns].replace(replace_text)
        else:
              return data[index_columns].replace(replace_text)



def mask_where(data:pd.DataFrame,index_columns:list,cond:bool,new_value:any,logic:str):
    if type(new_value)!=type([]):
     match logic:
      case "where":
        return data[index_columns].where(cond,new_value)
      case "mask":
       return data[index_columns].mask(cond,new_value)
      case _:
        return("not support logic")
    else:
      print("not support list type")
      



def sorting_data(data:pd.DataFrame,index_columns:list|None,set_index:list,ascending:bool|list):
    if index_columns is None:
      return data.sort_values(set_index,ascending=ascending)
    else:
     print('index_columns is not None')
     if duplicated_data_includ_test(data=data,index=index_columns,index_another=set_index):
      print('duplicated data finish')
      if type(ascending)==type([]):
        if len(ascending)==len(set_index):
         return data[index_columns].sort_values(set_index,ascending=ascending)
        else:print("ascending and set_index length is not equal")
      else:return data[index_columns].sort_values(set_index,ascending=ascending)
     else :print("set index is not in index_columns")
     




def data_set_index(data:pd.DataFrame,index_columns:list,set_indx:list):
    if duplicated_data_includ_test(data=data,index=index_columns,index_another=set_indx):
        return data[index_columns].set_index(set_indx)
    else:print("set index is not in index_columns")



def apply_data(data:pd.DataFrame|None,index_columns:list|pd.DataFrame|None,value,other_value=None,method:str|None=None): #value is anonymity function
    match method:
     case _:
      if   data is  not None:
        return data[index_columns].apply(value) 
      elif data is not None and index_columns is None:
        return data.expanding().apply(value)
      else : 
        return index_columns.apply(value,other_value)     






def rolling_apply_data(data:pd.DataFrame,index_columns:list|None,window:int,func:any,method:str|None=None,other:pd.Series|None=None,value:int|None=None):
  if window>-0: 
    if method is not None:
      match method:
       case 'cov':
          return data[index_columns].rolling(window).cov(data[other])
       case'corr':
         return data[index_columns].rolling(window).corr(data[other])
       case 'shfit':
        return data[index_columns].shift(value)
       case 'diff':
        return data[index_columns].diff(value)
       case 'pct':
        return data[index_columns].pct_change()
       case 'exponential':return data.rolling(window).apply(func)
    else:    
     return data[index_columns].rolling(window).apply(func)
  else:print("window is not negative")








