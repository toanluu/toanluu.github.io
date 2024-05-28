# encoding: utf-8

import re
import unicodedata
from xml.etree import ElementTree

import regex

"""
Simple text normalization utility. Used to tokenize, folding, remove stop words
"""

# Reference of map: https://docs.oracle.com/cd/E29584_01/webhelp/mdex_basicDev/src/rbdv_chars_mapping.html
DIACRITICAL_CHAR_MAP = {
    'Æ': 'A',
    'Đ': 'D',
    'đ': 'd',
    'ø': 'o',
    'Ð': 'D',
    'Ø': 'O',
    'þ': 'P',
    'Þ': 'p',
    'ß': 's',
    'ð': 'd',
    'æ': 'a',
    'Ħ': 'H',
    'ħ': 'h',
    'ı': 'i',
    'Ĳ': 'I',
    'ĳ': 'i',
    'ĸ': 'K',
    'l': 'l',
    'Ŀ': 'L',
    'ŀ': 'l',
    'Ł': 'L',
    'ł': 'l',
    'Ŋ': 'N',
    'ŋ': 'n',
    'ŉ': 'n',
    'Œ': 'O',
    'œ': 'o',
    'ſ': 's',
    'Ŧ': 'T',
    'ŧ': 't'
}

TRANS = str.maketrans(DIACRITICAL_CHAR_MAP)


def normalise_to_ascii(text):
    """
    Strip accent from the text, e.g: ü => u, é=>e
    """
    text = unicodedata.normalize("NFD", text)
    text = regex.sub(r"\p{Mark}+", "", text, flags=regex.V1)
    return text.translate(TRANS)


def tokenized(text, stopwords=None, lowercase=True, folding=None, max_words=None, synonyms=None):
    """
    Split a text to a list of tokens
    """
    if text is None:
        return None
    pattern = re.compile(r"\W+", re.UNICODE)
    tokens = []
    if folding:
        text = normalise_to_ascii(text)
    if lowercase:
        text = text.lower()
    for token in pattern.split(text):
        if token and (not stopwords or token not in stopwords):
            if synonyms is not None:
                synonym = synonyms.get(token)
                if synonym:
                    token = synonym

            tokens.append(token)
            if max_words and len(tokens) >= max_words:
                break
    return tokens


def normalized(text, blacklist_patterns=None, stopwords=None, lowercase=True, folding=None,
               max_words=None, delimiter=' ', synonyms=None):
    """
    Simple normalization of the text by folding, convert to lower, remove
    pattern remove stopwords
    """
    if blacklist_patterns:
        generic_pattern = '(%s)' % '|'.join(blacklist_patterns)
        pattern = re.compile(generic_pattern)
        text = pattern.sub(' ', text)
    tokens = tokenized(text, stopwords=stopwords, lowercase=lowercase, folding=folding, max_words=max_words,
                       synonyms=synonyms)
    return delimiter.join(tokens)


def is_safe_encoding(s):
    """Check string contain only alpha digit assci and _ for safe encoding"""
    return bool(re.match(r"^[A-Za-z0-9_]+$", s))


def get_uri(text):
    return '-'.join(tokenized(text, folding=True))


def get_fingeprint(text):
    """
    return finger print of a text, used for simple deduplicate
    """
    return '_'.join(sorted(tokenized(text, folding=True)))


def concat_tokenized(text, stopwords=None, folding=None, max_words=None, synonyms=None):
    """
    return concatenation of tokens after apply tokenization
    """
    return ''.join(tokenized(text, stopwords=stopwords, folding=folding, max_words=max_words, synonyms=synonyms))


def jaccard(text1, text2, stopwords=None, folding=True, synonyms=None):
    """
    Compute jaccard  similarity between 2 texts: https://en.wikipedia.org/wiki/Jaccard_index
    :return: value between 0-1
    """
    if not text1 or not text2:
        return 0

    s1 = set(tokenized(text1, folding=folding, stopwords=stopwords, synonyms=synonyms))
    s2 = set(tokenized(text2, folding=folding, stopwords=stopwords, synonyms=synonyms))
    overlap = s1.intersection(s2)
    union = s1.union(s2)
    return len(overlap) * 1.0 / len(union)


def clean_html(raw_html):
    """
    Clean html tags from a text
    """
    return ''.join(ElementTree.fromstring(raw_html).itertext())


def get_ronto_id(name):
    if name is None:
        return None
    return normalized(name, lowercase=False, folding=True, delimiter='_')
