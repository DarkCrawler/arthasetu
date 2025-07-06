import calendar
from io import BytesIO

import requests
from google.cloud import storage


def generate_day_with_suffix(last_day):
    '''
    simpler explanation for return f"{last_day}{'th' if 10 <= last_day % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(last_day % 10, 'th')}".lower()
    # Step 1: Handle special cases (11th, 12th, 13th)
    if 10 <= day % 100 <= 20:
        suffix = "th"

    # Step 2: Handle normal suffix rules based on last digit
    else:
        last_digit = day % 10  # get the last digit
        if last_digit == 1:
            suffix = "st"
        elif last_digit == 2:
            suffix = "nd"
        elif last_digit == 3:
            suffix = "rd"
        else:
            suffix = "th"


    '''

    return f"{last_day}{'th' if 10 <= last_day % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(last_day % 10, 'th')}".lower()


def generate_file_names_for_year(year: int):
    filenames = []
    for month in range(1, 13):
        last_day = calendar.monthrange(year, month)[1]
        day_with_suffix = generate_day_with_suffix(last_day)
        month_name = calendar.month_name[month].lower()
        filenames.append(f"all-schemes-monthly-portfolio---as-on-{day_with_suffix}-{month_name}-{year}.xlsx")
    return filenames


def check_available_files():
    file_names_for_year = generate_file_names_for_year(2025)
    search_url_string = "https://www.sbimf.com/docs/default-source/scheme-portfolios/"
    available_files = {}
    for filename in file_names_for_year:
        search_url = search_url_string + filename
        print(f'searching for ...{search_url}')
        response = requests.get(search_url)
        content_disposition = response.headers.get('Content-Disposition')

        if content_disposition:
            available_files[filename] = search_url
        else:
            print(f'file .. {filename} not present, stopping file scan')
            break

    return available_files


def download_file_to_gcs(content: bytes, filename: str, content_type: str):
    client = storage.Client()
    bucket_name = 'projx-tb-extracts-staging'
    bucket = client.bucket(bucket_name)

    blob = bucket.blob('pxsbi/' + filename)
    blob.upload_from_file(BytesIO(content), content_type=content_type)
    gcs_file_path = f'gs://{bucket_name}/{filename}'
    print(f"Uploaded: {gcs_file_path}")
    return gcs_file_path


def download_files_to_gcs_dir(available_file_uris):
    for file_name, file_uri in available_file_uris.items():
        print(f'{file_name} .... {file_uri}')
        response = requests.get(file_uri)
        print(f'response status ... {response.status_code}')
        gcs_file_path = download_file_to_gcs(response.content, file_name,
                                             response.headers.get("Content-Type", "application/vnd.ms-excel"))
        print(f'file uploaded to gcs : {gcs_file_path}')


if __name__ == "__main__":
    # available_file = check_available_files()
    # print(available_file)
    available_files = {
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-january-2025.xlsx': 'https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-january-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx': 'https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-march-2025.xlsx': 'https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-march-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-30th-april-2025.xlsx': 'https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-30th-april-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-may-2025.xlsx': 'https://www.sbimf.com/docs/default-source/scheme-portfolios/all-schemes-monthly-portfolio---as-on-31st-may-2025.xlsx'
    }

    download_files_to_gcs_dir(available_files)
