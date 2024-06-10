import pandas as pd
import requests
import time
import random
import numpy as np

fields = "can_see_audio,bdate,can_send_friend_request,can_write_private_message,career,counters, common_count,connections,contacts,city,country,crop_photo,domain,education,exports,followers_count,friend_status,has_photo,has_mobile,home_town,sex,site,schools,screen_name,status,verified,games,interests,is_favorite,is_friend,is_hidden_from_feed,last_seen,maiden_name,military,movies,music,nickname,occupation,online,personal,photo_id,photo_max,photo_max_orig,schools,quotes, sex, status, wall_default, verified about,relation,relatives,timezone,tv, universities"
token = 'my_token'
#method = 'groups.get'
version = '5.137'


def get_vk_data(method, params, max_attempts=5):
    url = 'https://api.vk.com/method/{method}?{params}&access_token={token}&v={version}'

    url = url.format(method=method, params=params, token=token, version=version)
    attempt = 0
    while attempt < max_attempts:
        resp = requests.get(url)
        # print(resp.json())
        data = resp

        if 'error' in data.json() and (data.json()['error'].get('error_code') == 6 or data.json()['error'].get('error_code') == 6):
            time.sleep(4)
            attempt += 1
            continue

        break

    return data.json()


def get_post(group_id):
    try:
        print(group_id)
        resp = get_vk_data('groups.getById', 'group_id={}&fields=description'.format(group_id))['response']
        id = resp[0]['id']
        description = resp[0]['description']
        print(description)
        print(id)
        resp = get_vk_data('wall.get', 'owner_id=-{}'.format(id))['response']
        n = resp['count']
        # print(rec2groups)
        print("В группе {} должно быть {} постов".format(group_id, n))
        post_id = pd.DataFrame(columns = ['group_id','description','post_id','text','date'])
        for i in (range(0, n + 100, 100)):
            wall = get_vk_data('wall.get', 'owner_id=-{}&count=100&offset='.format(id) + str(i))['response']['items']
            # print(wall)
            post = pd.DataFrame([(group_id, description.replace('\n', ' ').replace('\r', ' '), item['id'], item['text'].replace('\n', ' ').replace('\r', ' '), item['date']) for item in wall], columns = ['group_id','description','post_id','text','date'])
            post_id = pd.concat([post, post_id], ignore_index=True)
            print(i)

        return post_id
    except Exception:
        return pd.DataFrame()


def main():
    data = pd.DataFrame()

    target_groups = {


    }

    data = pd.DataFrame()
    for name in target_groups:
        group_id = target_groups[name]
        data = get_post(group_id)

        filename = 'data_' + group_id + '.csv'
        # filename = 'test.csv'
        print(filename)
        data.to_csv(filename)
    #

if __name__ == '__main__':
    main()