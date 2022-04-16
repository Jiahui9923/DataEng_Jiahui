## File Declaration

All code implementaations are executed in **DataGathering.ipynb** file.

All notes are recoreded in the **Note.pdf** file.

## Basic grammar record

### Ptyhon ###
1. import xxx as xxx

```python
# Call some base library or other resource.
import pandas as pd
import numpy as np
# Inline presentation method
%matplotlib inline
```
2. url

```python
url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)
```
### BeautifulSoup ###
1. how to use BeautifulSoup() function
```python
soup = BeautifulSoup(html,'lxml')

#to show the type of soup
type(soup)
# bs4.BeautifulSoup
```
2. The basic grammar
  
  - .get_text()
  - .fin_all('a')
3. Example: To remove html tags using BeautifulSoup

```python
str_cells = str(row_td)
cleantext = BeautifulSoup(str_cells, "lxml").get_text()
print(cleantext)
```
### Pandas Dataframe ###
1. dataframe

```python
df = pd.DataFrame(lists_rows)
# .head() show 10 rows
# .head(x) show x rows
df.head(10)
```
2. .split('x', expand = T/F)

```python
df1 = df[0].str.split(',',expand = True)
```

3. overview of the data 

```python
df5.info()
df5.shape
```

4. The basic grammar

```python
df5.dropna(axis = 0, how = 'any')
df6.drop(df6.index[0])
df7.rename(columns={'[Place]:]Place]'},inplace = True)
df7.describe(include = [np.number])
```
### Data Analysis and Visualization ###

1. All concrete code implementations are executed in files
2. The basic grammar

  - .boxplot
  -  .grid
  -  .plt.ylabel || plt.xticks
  -  sns.distplot()
