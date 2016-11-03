package com.spark.usecases.twitteranalytics

import twitter4j.Twitter
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
class twitterclient {
  val CONSUMER_KEY: String = "Tn6mCikBNxLviA6znN4FgIXfY"
  val CONSUMER_KEY_SECRET: String = "JoRN26wNoPUuUYsgR4zKwre82zTY53r8rDzy6nLSrS4cMqiRzg"
  val ACCESS_TOKEN = "199435611-ancQT2HKivvIrlrKg2FYLTBoQyA0zsISGhDbO7ug"
  val ACCESS_TOKEN_SECRET = "wHaw4X7ok2uWXVGvOAOzaSgZvRovK4xFY4CAMLoNuMOy8"
  def start(): Twitter = {
    val twitter: Twitter = new TwitterFactory().getInstance();
    twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
    twitter.setOAuthAccessToken(new AccessToken(ACCESS_TOKEN, ACCESS_TOKEN_SECRET))
    twitter
  }
}