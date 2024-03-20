# Code files

You should be able to find all the code files used in this project ready for execution after minor adjustments.
The folder is composed of: 

1.Scrapping_funcError.py
Contains a function that scraps all messages from StockTwits for given assets published before a cut-off date. It stores them in a MongoDB database. It allows to automatically handle errors in the scraping, notably due to blockages of the API after extracting large volumes of data. 

2.CleanDataset.py 
Remove bots/ users on 0.1 percentile of activity 
remove stock names, links, user mentions 
 