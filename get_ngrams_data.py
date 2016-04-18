#!/usr/bin/python
from multiprocessing import Pool
import urllib
import gzip
import os

def get_1gram_data_and_parse(letter):
    url = 'http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-1gram-20120701-' + letter + '.gz'
    filename = 'google-1gram-' + letter + '.gz'
    if not os.path.isfile(filename):
        print 'Getting letter \'' + letter + '\' data...'
        urllib.urlretrieve(url, filename)
    else:
        print 'Using existing download for letter \'' + letter + '\''
    word_occurrances = dict()
    with gzip.GzipFile(filename) as f:
        for line in f:
            parts = line.split('\t')
        if len(parts) == 4:
            word = parts[0].upper()
            year = int(parts[1])
            occurances = int(parts[2])
            if year > 2000:
                try:
                    word_occurrances[word] += occurances
                except KeyError:
                    word_occurrances[word] = occurances
    return word_occurrances

if __name__ == '__main__':
    p = Pool(8)
    letters = map(chr, range(97, 123))
    print 'Processing 1-gram data from Google'
    first = True
    word_occurances = dict()
    for d in p.imap(get_1gram_data_and_parse, letters):
        if first:
            word_occurances = d
            first = False
            continue
        else:
            for (k,v) in d.items():
                try:
                    word_occurances[k] += v
                except KeyError:
                    word_occurances[k] = v
    with open('google-1gram-data.txt', 'w+') as f:
        for (k,v) in word_occurances.items():
            f.write(k + ',' + str(v) + '\n')
