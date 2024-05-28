# encoding: utf-8
"""
Simple text normalization utility. Used to remove stop words and fold special characters
"""
import re
import unicodedata


def get_stopwords_vi():
    s = set()
    with open('resources/stopwords_vi.txt', mode='rt', encoding='utf-8') as f:
        for l in f.readlines():
            s.add(l.strip())
    return s


def fold(text):
    """
    Strip accent from the text, e.g: ü => u, é=>e
    """
    s = ''
    for c in unicodedata.normalize('NFD', text):
        if c == 'đ' or c == 'ð':
            s = s + 'd'
        elif unicodedata.category(c) != 'Mn':
            s = s + c
    return s


def tokenized(text, stopwords=None, folding=None, max_words=None):
    """
    Split a text to a list of tokens
    """
    if text is None:
        return None
    pattern = re.compile(r"\W+", re.UNICODE)
    tokens = []
    if folding:
        text = fold(text)
    for token in pattern.split(text.lower()):
        if token and (not stopwords or token not in stopwords):
            tokens.append(token)
            if max_words and len(tokens) >= max_words:
                break
    return tokens


def normalized(text, blacklist_patterns=None, stopwords=None, folding=None,
               max_words=None, delimiter=' '):
    """
    Simple normalization of the text by folding, convert to lower, remove
    pattern remove stopwords
    """
    if blacklist_patterns:
        generic_pattern = '(%s)' % '|'.join(blacklist_patterns)
        pattern = re.compile(generic_pattern)
        text = pattern.sub(' ', text)
    tokens = tokenized(text, stopwords=stopwords, folding=folding, max_words=max_words)
    return delimiter.join(tokens).lower()


def get_ngrams(text, n, stopwords=None, folding=None):
    tokens = tokenized(text, stopwords=stopwords, folding=folding, max_words=None)
    ngrams = []
    for i in range(0, len(tokens) - n + 1):
        ngrams.append(' '.join(tokens[i:i + n]))
    return ngrams


def get_all_ngrams(text, stopwords=None, folding=None):
    tokens = tokenized(text, stopwords=stopwords, folding=folding, max_words=None)
    ngrams = []
    for n in reversed(range(1, len(tokens) + 1)):
        for i in range(0, len(tokens) - n + 1):
            ngrams.append(' '.join(tokens[i:i + n]))
    return ngrams


def get_uri(text):
    return '-'.join(tokenized(text, folding=True))


def get_fingerprint(text):
    """
    return finger print of a text, used for simple deduplicate
    """
    return '_'.join(sorted(tokenized(text, folding=True)))


def get_jaccard(text1, text2, stopwords=None):
    words1 = tokenized(text1, stopwords=stopwords)
    words2 = tokenized(text2, stopwords=stopwords)

    return get_jaccard_from_words(words1, words2, stopwords=stopwords)


def get_jaccard_from_words(words1, words2, stopwords=None):
    if not words1 or not words2:
        return 0

    s1 = set(words1)
    s2 = set(words2)
    overlap = s1
    overlap = overlap.intersection(s2)
    union = s1
    union = union.union(s2)

    return len(overlap) * 1.0 / len(union)


def no_accent_vietnamese(s):
    s = re.sub(u'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(u'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(u'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(u'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(u'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(u'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(u'[ìíịỉĩ]', 'i', s)
    s = re.sub(u'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(u'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(u'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(u"[ỳýỵỷỹ]", 'y', s)
    s = re.sub(u'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(u'[Đ]', 'D', s)
    s = re.sub(u"[đ]", 'd', s)
    return s


def create_slug(_id, _name):
    _id = str(_id).strip()
    _name = remove_special_char(_name)
    return f"{_name}-{_id}"


def remove_special_char(_text):
    _text = re.sub(' +', ' ', _text)
    _text = re.sub(r'[^\w0-9- ]+', "", _text)
    _text = fold(re.sub(r'\s', "-", _text).lower())
    _text = re.sub('[-]+', '-', _text)
    return _text