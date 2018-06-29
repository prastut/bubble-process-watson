hashtag = raw_input("Please enter the hashtag for which you want to stream the match.\n")
text_file = open("hashtag.txt", "w")
text_file.write(hashtag)
text_file.close()