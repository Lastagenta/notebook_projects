import requests

def main():
    url = 'url'

    links=[]

    with open('check.txt','rt') as f:
        links = f.readlines()

    if len(links)==0:
        print ('Нету ссылок для проверки')

    for link in links:
        link=link.replace('\n', '')
        full_link = ''.join((url,link))
        response = request.get(full_link)

        if response != 404:
            print(f'{full_link} - существует')
          
if __name__ == "__main__":
    main()
