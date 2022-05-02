import requests

CNN_RSS = 'https://www.cnn.com/services/rss/'
CNN_MONEY_RSS = 'https://money.cnn.com/services/rss/'

cnn_rss_html = requests.get(CNN_RSS).text

with open('data/cnn_rss_html.html', 'w') as f:
    f.write(cnn_rss_html)

cnn_money_rss_html = requests.get(CNN_MONEY_RSS).text

with open('data/cnn_money_rss_html.html') as f:
    f.write(cnn_money_rss_html)
