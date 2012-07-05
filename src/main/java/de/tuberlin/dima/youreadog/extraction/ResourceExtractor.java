package de.tuberlin.dima.youreadog.extraction;

import com.google.common.collect.Sets;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceExtractor {

  private final Pattern resourcePattern = Pattern.compile(
      "\\b(((ht|f)tp(s?)\\:\\/\\/|~\\/|\\/)|www.)" +
          "(\\w+:\\w+@)?(([-\\w]+\\.)+(com|org|net|gov" +
          "|mil|biz|info|mobi|name|aero|jobs|museum" +
          "|travel|[a-z]{2}))(:[\\d]{1,5})?" +
          "(((\\/([-\\w~!$+|.,=]|%[a-f\\d]{2})+)+|\\/)+|\\?|#)?" +
          "((\\?([-\\w~!$+|.,*:]|%[a-f\\d{2}])+=?" +
          "([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)" +
          "(&(?:[-\\w~!$+|.,*:]|%[a-f\\d{2}])+=?" +
          "([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)*)*" +
          "(#([-\\w~!$+|.,*:=]|%[a-f\\d]{2})*)?\\b");

  private final URLNormalizer urlNormalizer = new URLNormalizer();

  public Set<String> extractScripts(String sourceUrl, String html) {

    if (sourceUrl == null) {
      return Collections.emptySet();
    }

    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Set<String> resources = Sets.newHashSet();

    Matcher matcher = resourcePattern.matcher(html);
    while (matcher.find()) {
      String resource = matcher.group();

      //TODO should be included in pattern
      resource = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, resource);

      //TODO normalize stuff like index.html, default.aspx, ...
      //TODO remove php session id etc

      // normalize link
      try {
        resource = urlNormalizer.normalize(resource);
      } catch (MalformedURLException e) {
        System.out.println("Unable to process resource: " + resource);
      }

      resources.add(resource);
    }


    return resources;
  }

}
