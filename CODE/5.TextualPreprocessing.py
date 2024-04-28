# -*- coding: utf-8 -*-
"""
Machine Learning - Other Algorithms

@author: ncardenafria
"""

# Connect to MongoDB
uri = "mongodb+srv://nataliacardenas:stocktwits@twits.mgv4dfh.mongodb.net/?retryWrites=true&w=majority&appName=Twits"
client = MongoClient(uri)
db = client["StockTwits"]
df = pd.DataFrame(list(db["twits"].find({"sentiment": {"$ne": ""}}, {"content": 1, "sentiment": 1})))
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')

def apply_pos_tags(text):
    words = word_tokenize(text)
    pos_tags = pos_tag(words)
    return ' '.join([f"{word}_{tag}" for word, tag in pos_tags])

def emojis_and_punctuation(text):
    text = emoji.demojize(text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

# Function to preprocess, train, and evaluate
def preprocess_train_evaluate(preprocess_fn, df, vectorizer, model):
    df['processed_content'] = df['content'].apply(preprocess_fn)
    X = vectorizer.fit_transform(df['processed_content'])
    y = df['sentiment']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scores = cross_validate(model, X_train, y_train, scoring=['accuracy', 'matthews_corrcoef'], cv=10)
    accuracy = scores['test_accuracy'].mean()
    mcc = scores['test_matthews_corrcoef'].mean()
    return accuracy, mcc

vect = CountVectorizer(stop_words="english") 
bayes = MultinomialNB()

results = {}
results['Benchmark'] = preprocess_train_evaluate(lambda x: x, df, vect, bayes)
results['Punctuation'] = preprocess_train_evaluate(remove_punctuation, df, vect, bayes)
results['Stem'] = preprocess_train_evaluate(stem_text, df, vect, bayes)
results['Emoticons'] = preprocess_train_evaluate(handle_emojis, df, vect, bayes)
results['StopWords'] = preprocess_train_evaluate(remove_stopwords, df, vect, bayes)
results['PosTagging'] = preprocess_train_evaluate(apply_pos_tags, df, vect, bayes)
results['Emojis + Punctuation'] = preprocess_train_evaluate(emojis_and_punctuation, df, vect, bayes)

results_df = pd.DataFrame(results, index=['Accuracy', 'MCC']).T
print(results_df)
