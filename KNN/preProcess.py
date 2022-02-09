def preProcess():
  """ This function normalizes the dataset using MinMax Scaler and returns normalized csv dataset.

  Returns:
      DataFrame: Normalized Data of the Iris.csv dataset
  """
  from sklearn.preprocessing import MinMaxScaler
  import pandas as pd
  import sys
  df = pd.read_csv(sys.argv[-1])
  df_n = df.copy()
  min_max_scaler = MinMaxScaler()
  df_n.iloc[:, [1, 2, 3]] = min_max_scaler.fit_transform(df_n.iloc[:, [1, 2, 3]])
  df_n.to_csv('normalizedIris.csv',index=False)
  return df_n

def fetchLabels():
  """ This function splits the normalized data into training and test data.

  Returns:
      DataFrame: Train and Test Data respectively
  """
  dataset = preProcess()
  labels = [dataset.loc[dataset[i].notnull(), :] for i in dataset.columns if i[0]!="Id"][-1]
  return labels

def eucledian_dist(alist, blist):
  """ This function calculates the eucledian distance between two points.

  Args:
      alist ([list]): Train data
      blist ([list]): Test data

  Returns:
      float: Eucledian Value of the two points
  """
  return sum((float(x) - float(y)) ** 2 for (x, y) in zip(alist, blist)) ** 0.5

preProcess()