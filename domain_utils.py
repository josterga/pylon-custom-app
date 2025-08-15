import json
import re
from sklearn.feature_extraction.text import TfidfVectorizer

STOPWORDS = set("""
a about above after again am an and are as at be because been before being below but by can did do does doing down further had has have having he her here hers herself him himself his how i ideally if in is it its itself just me my myself no nor not of on or other our ours ourselves out own same she should so some such than that the their theirs them themselves then there these they this those through to too under until up very was we were what when where which while who whom why will with you your yours yourself yourselves please thanks hi hello regards note see ask wanted should could would know let make get new set use work issue show think look found question want need help appreciate attached sent send sending replied reply replying regards sincerely best
""".split())

def flatten_and_filter_keywords(keyword_list, stopwords):
    flat_keywords = set()
    for item in keyword_list:
        if isinstance(item, str):
            if item not in stopwords:
                flat_keywords.add(item)
        elif isinstance(item, list) and item and isinstance(item[0], str):
            if item[0] not in stopwords:
                flat_keywords.add(item[0])
    return flat_keywords

def flatten_and_filter_phrases(phrase_list, stopwords):
    flat_phrases = set()
    for item in phrase_list:
        if isinstance(item, str):
            if not any(w in stopwords for w in item.split()):
                flat_phrases.add(item)
        elif isinstance(item, list) and item and isinstance(item[0], str):
            if not any(w in stopwords for w in item[0].split()):
                flat_phrases.add(item[0])
    return flat_phrases

def load_domain_sets(path):
    with open(path, "r") as f:
        data = json.load(f)
    keywords = flatten_and_filter_keywords(data.get("keywords", []), STOPWORDS)
    phrases = flatten_and_filter_phrases(data.get("phrases", []), STOPWORDS)
    return keywords, phrases

def extract_keywords(text, min_len=3, stopwords=STOPWORDS):
    tokens = re.findall(r'\b\w+\b', text.lower())
    return [t for t in tokens if t not in stopwords and len(t) >= min_len]

def extract_weighted_domain_ngrams(text, domain_keywords, domain_phrases, ngram_sizes=(3,2,1)):
    vocabulary = list(domain_keywords | domain_phrases)
    vectorizer = TfidfVectorizer(
        ngram_range=(1, max(ngram_sizes)),
        stop_words=None,
        vocabulary=vocabulary
    )
    tfidf_matrix = vectorizer.fit_transform([text.lower()])
    feature_names = vectorizer.get_feature_names_out()
    tfidf_scores = tfidf_matrix.toarray()[0]
    return {feature_names[i]: tfidf_scores[i] for i in range(len(feature_names)) if tfidf_scores[i] > 0}
