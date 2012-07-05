package de.tuberlin.dima.youreadog.extraction;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LinkExtractor {

  private final Pattern linksPattern =
      Pattern.compile("<a\\s[^>]*href\\s*=\\s*\\\"([^\\\"]*)\\\"[^>]*>.*</a>", Pattern.CASE_INSENSITIVE);

  private final URLNormalizer urlNormalizer = new URLNormalizer();

  public Set<String> extractLinks(String sourceUrl, String html) {

    if (sourceUrl == null) {
      return Collections.emptySet();
    }

    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Set<String> links = Sets.newHashSet();

    Matcher matcher = linksPattern.matcher(html);
    while (matcher.find()) {
      String link = matcher.group(1);
      if (isPageLink(link)) {
        try {

          link = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, link);

          //TODO normalize stuff like index.html, default.aspx, home.htm, index.htm ...
          //TODO remove php session id etc

          // normalize link
          link = urlNormalizer.normalize(link);

          links.add(link);
        } catch (Exception e) {
          //TODO handle stuff like http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;ftr3
          //System.out.println("Unable to process link: " + link);
        }
      }
    }

    return links;
  }

  private boolean isPageLink(String link) {
    return !link.startsWith("#") && !link.startsWith("mailto:") && !link.startsWith("javascript:");
  }

}
