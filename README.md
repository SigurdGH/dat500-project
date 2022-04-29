# dat500-project
Project in DAT500 

This project is based on what we did in a previous assignment in DAT550. The file took a long time to run, so that is why we are now using a Spark cluster to run it in a substancially shorter time.

We got the datasets from Kaggle (https://www.kaggle.com/datasets/sbhatti/news-summarization) (3.83 GB) and from Vinay Setty (professor at the University of Stavanger). You can locate the latter from the assignment2 file.

We also created a python file (dataToCsv.ipynb) to extract 1000, 10000 and 25000 articles from the assignment 2 dataset.
You can see the 1000 articles sample in /data.

All datasets that got used was uploaded to every node and the sparkLSH.py was uploaded and ran on the master node.
