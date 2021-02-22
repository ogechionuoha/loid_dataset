from __future__ import print_function
import csv, multiprocessing, cv2, os
import numpy as np
import urllib.request

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"

opener = AppURLopener()
def url_to_image(url):
    resp = opener.open(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_UNCHANGED)
    return image


def download_and_resize(country, im_id, im_url):
    try:
        save_dir = os.path.join('./images/', country)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        save_path = os.path.join(save_dir,im_url.split('/')[-1])

        if os.path.isfile(save_path):
            save_path = os.path.join(save_dir,im_id,im_url.split('/')[-1])

        if not os.path.isfile(save_path):
            print(save_path, im_url)
            img = url_to_image(im_url)
            cv2.imwrite(save_path,img)
        else:
            print('Already saved: ' + save_path)
    except Exception as e:
        print(e)
        with open("./log/bad.txt", "a") as bad:
            bad.write(save_path)
            bad.write("\n")

def main():
    country_dir = './countries/'
    for filename in os.listdir(country_dir):
        
        country = filename.strip('.csv')

        print('Processing...', country)

        if country=='':
            country = 'undefined'

        with open(country_dir+filename, 'r') as train_f:
            train_reader = csv.reader(train_f)
            header = train_reader.__next__()
            pool = multiprocessing.Pool(processes=2*multiprocessing.cpu_count())
            results = [pool.apply_async( download_and_resize, [ country, image_id, image_data[0] ] )
                                    for image_id, image_data in enumerate(train_reader)]
            pool.close()
            pool.join()
        
        break




if __name__ == '__main__':
    main()