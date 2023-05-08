import requests

# Kiểm tra xem node hiện tại có bao nhiêu Index

# Khai báo địa chỉ của node (localhost:9200)
url = "http://192.168.1.99:9200/_cat/indices"
# gọi API để lấy danh sách index
response = requests.get(url)
print(response.text)