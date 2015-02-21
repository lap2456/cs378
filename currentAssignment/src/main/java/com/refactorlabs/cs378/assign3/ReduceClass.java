package com.refactorlabs.cs378.assign3;

import java.io.IOException;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for InvertedIndex.  Extends class Reducer, provided by Hadoop.
 * @author Louis Pujol (louispujol@yahoo.com)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashSet<String> references = new HashSet<String>();

		//go through all of the various book:chapter:verse references and
		//add them to the HashSet to avoid duplicates
		for(Text value : values){
			//split all of the references on the space character
			String[] allStrings = value.toString().split(" ");
			for(String str : allStrings){
				//if string is empty then do not add
				if(str.length() > 0)
					references.add(str);
			}
		}

		List<String> results = sortResults(references);

		//create text object for output
		Text output = new Text();
		//join all of the chapter:book:verse references with a comma
		String outputString = join(results, ",");
		//set this string as the Text object and write to context
		output.set(outputString);
		context.write(key, output);
	}

   /*
	* Will join a Set of strings together with a seperator
	* and then will delete the trailing seperator
	* 
	* @param token: the given token that will be cleaned
	*/
	public static String join(List<String> words, String seperator){
		//using a stringbuilder for time efficiency
		StringBuilder result = new StringBuilder();
		for(String word : words){
			if(word.length() > 0)
				result.append(word + seperator);
		}
		//find the last seperator (ex. ',') and remove it so that there is no trailing one
		if(result.length() > 0)
			result.delete(result.lastIndexOf(seperator), result.length());
		return result.toString();
	}

   /*
	* After the Set is lexographically sorted then the entries should already be sorted
	* by book and within the books the chapters should be grouped together by their numbers
	* 
	* ex) if we had 2 books Matthew and John, each have chapters 1-4 then after lexographically
	* sorting, we can assume that all of the chapters and verses in Matthew are together and all
	* of the verses within chapters 1 are together as well as those for 2, 3 and 4. The same can
	* be known for those in John
	*
	* So the algorithm is to traverse the lexographically sorted list and we sort the list in chunks
	* if we hit either a new book or new chapter then we will sort the chunk that we have thus far and
	* add it to the resulting list
	*/
	public static List<String> sortResults(Set<String> words){
		List<String> list = new ArrayList<String>(words);
  		java.util.Collections.sort(list); //sort lexographically

  		Map<String, Map<Integer, List<String>>> bookToChapterToVerses = new TreeMap<String, Map<Integer, List<String>>>(); 
  		List<String> fullySortedList;
  		if(list.size() > 0){
  			int i=1;
  			Map<Integer, List<String>> mapForCurrentBook = new TreeMap<Integer, List<String>>();
  			String firstEntry = list.get(0);
  			String currentBook = getBook(firstEntry);
  			int currentChapter = getChapter(firstEntry);
  			int previousIndex = 0;
  			int maxVerse = getVerse(firstEntry);
  			//first sort chapters by verses
  			while(i<list.size()){
  				String currentEntry = list.get(i);
  				String thisBook = getBook(currentEntry);
  				int thisChapter = getChapter(currentEntry);
  				int thisVerse = getVerse(currentEntry);
  				//if we have a new book then we sort our chunk and add it to the chapter map
  				//then we add the chapter map to the book map and reset the chapter map
  				if(!thisBook.equals(currentBook)){
  					mapForCurrentBook.put(currentChapter, sortThisChapter(list.subList(previousIndex, i), maxVerse));
  					bookToChapterToVerses.put(currentBook, new TreeMap(mapForCurrentBook));
  					previousIndex = i;
  					currentBook = thisBook;
  					currentChapter = thisChapter;
  					maxVerse = thisVerse;
  					mapForCurrentBook = new TreeMap<Integer, List<String>>();
  				}
  				//if we have a new chapter then we sort our chunk and add it only to the chapter map
  				else if(thisChapter != currentChapter){
  					mapForCurrentBook.put(currentChapter, sortThisChapter(list.subList(previousIndex, i), maxVerse));
  					previousIndex = i;
  					currentChapter = thisChapter;
  					maxVerse = thisVerse;
  				}
  				//otherwise just check if we have found a new max verse for this chapter
  				else{
  					if(thisVerse > maxVerse){
  						maxVerse = thisVerse;
  					}
  				}
  			++i;
  			}
  			//once we are through going through the list we still have a chunk left to be sorted
  			mapForCurrentBook.put(currentChapter, sortThisChapter(list.subList(previousIndex, i), maxVerse));
  			bookToChapterToVerses.put(currentBook, new TreeMap(mapForCurrentBook));

  			//now sort the books by chapters
  			List<String> bookList = new ArrayList<String>(bookToChapterToVerses.keySet());
  			java.util.Collections.sort(bookList);
  			fullySortedList = new ArrayList<String>(list.size());
  			//go alphabetically through books
  			for(String book : bookList){
  				Map<Integer, List<String>> chapterToBook = bookToChapterToVerses.get(book);
  				List<Integer> chapterList = new ArrayList<Integer>(chapterToBook.keySet());
  				java.util.Collections.sort(chapterList);
  				//then add chapters in numerical order. the verses are already sorted
  				for(Integer chapter : chapterList){
  					fullySortedList.addAll(chapterToBook.get(chapter));
  				}
  			}
  		}
  		//if we somehow got an empty set then we return an empty list
  		else{
  			fullySortedList = new ArrayList<String>();
  		}
  		return fullySortedList;
	}

   /* 
    * while iterating through the chapter we have kept track
    * we will create an array where the verse number is our index
    * then we transform that array into an ArrayList and get rid of any nulls( missing verses )
    * finally we return our sorted ArrayList
    */
	public static List<String> sortThisChapter(List<String> words, int maxVerse){
		String[] sortedWords = new String[maxVerse];
		for(String str : words){
			sortedWords[getVerse(str) - 1] = str;
		}
		List<String> sortedList = new ArrayList<String>(Arrays.asList(sortedWords));
		//get rid of nulls (missing verses)
		while(sortedList.contains(null)){
			sortedList.remove(null);
		}
		return sortedList;
	}

	//this will return the book from a book:chapter:verse reference
	public static String getBook(String reference){
		String[] args = reference.split(":");
		return args[0];
	}

	//this will return the chapter from a book:chapter:verse reference
	public static int getChapter(String reference){
		String[] args = reference.split(":");
		return Integer.parseInt(args[1]);
	}

	//this will return the verse from a book:chapter:verse reference
	public static int getVerse(String reference){
		String[] args = reference.split(":");
		return Integer.parseInt(args[2]);
	}
}