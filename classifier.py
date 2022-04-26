# Create a naive bayes text classifier of urls

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from urllib.parse import urlparse


def get_domain(url):
    parsed = urlparse(url)
    return parsed.path

def train_classifier(urls, urls_importance):
    # Split the urls into training and test sets
    urls_train, urls_test, y_train, y_test = train_test_split(urls, urls_importance, test_size=0.2)

    # Instantiate the pipeline
    pipeline = Pipeline([
        ('vect', CountVectorizer(ngram_range=(2, 3), analyzer="char_wb", max_features=100)),
        ('clf', MultinomialNB())
    ])

    # Fit the pipeline to the training set
    pipeline.fit(urls_train, y_train)

    # Use the pipeline to predict the test set
    predicted = pipeline.predict(urls_test)

    # Generate a classification report
    print(classification_report(y_test, predicted,
                                target_names=['skip', 'scrape']))

    # Return the pipeline
    return pipeline

# urls = [
#     'http://10printendruk.nl/over-ons',
#     'https://www.123afval.nl/contact',
#     'https://www.123ragers.nl/shop/sasd',
#     'http://101bhvshop.nl/hoi/moes'
# ]
# urls = [get_domain(url) for url in urls]
# pipeline = train_classifier(urls, [1, 1, 0, 0])



with open("data/test_classifier.tsv", "r") as f:
    urls = [get_domain(_) for _ in f.readlines()]
    urls = [_ for _ in urls if len(_) > 0]

urls = [part for domain in urls for part in domain.split('/') if len(part) > 0]


from collections import Counter
print(Counter(urls).most_common(50))






def get_urls_from_file(filename):
    with open(filename, 'r') as f:
        urls = f.read().splitlines()
    return urls

# if __name__ == '__main__':
#     urls = get_urls_from_file('urls_corporate.txt')
#     pipeline = train_classifier(urls)
#     url = 'https://www.google.com'
#     print

