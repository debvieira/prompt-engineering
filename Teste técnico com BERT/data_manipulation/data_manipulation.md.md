# Data Manipulation Project
Data manipulation project to visualize the following processes: text cleaning (removal of stopwords, normalization, and tokenization), lemmatization, and vector creation (TF-IDF and Bag of Words).

## Files
- data_manipulation.ipynb

### data_manipulation.ipynb
This script begins by importing the necessary libraries for text processing and data manipulation: NLTK, pandas, and tools from scikit-learn for vectorization. The NLTK resources for tokenization, stopword removal, and lemmatization are downloaded and utilized.
The data consists of a list of sentences, which will be processed through various steps.
The following functions are defined:
- `clean_text`: removes stopwords and non-alphabetic characters from the text, returning a list of clean words. This function tokenizes the text, converts it to lowercase, and filters out any non-alphabetic tokens and stopwords.
- `clean_texts`: applies the `clean_text` function to each sentence in the list, joining the cleaned words back into a string.
- `lemmatize_words`: uses NLTK's WordNetLemmatizer to lemmatize each word.
- `lemmatize_texts`: applies lemmatization and joins the lemmatized words back into a string.
For vectorization, two approaches are used: TF-IDF and Bag of Words.
- `create_tfidf_vectors`: creates TF-IDF vectors from the lemmatized texts using TfidfVectorizer.
- `create_bow_vectors`: creates Bag of Words vectors from the lemmatized texts using CountVectorizer.