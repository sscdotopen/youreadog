/**
 * Copyright (C) 2012 Database Systems and Information Management
 * Group of Technische Universität Berlin (http://www.dima.tu-berlin.de)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package de.tuberlin.dima.youreadog.extraction;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

import com.google.common.collect.Sets;

@Deprecated
public class LinkExtractor {

  private final Pattern linksPattern =
//      Pattern.compile("<a\\s[^>]*href\\s*=\\s*\\\"([^\\\"]*)\\\"[^>]*>.+</a>", 
//    		  Pattern.CASE_INSENSITIVE);
		  Pattern.compile("<a\\b[^>]*href=\"([^>]*)\">(.*?)</a>", 
			  Pattern.CASE_INSENSITIVE);

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
          //handle stuff like http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;ftr3	
          if(link.matches(".+&#[0-9]{2,};.+")) {
          	link = StringEscapeUtils.unescapeHtml(link);
          }
          link = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, link);
          // normalize link
          link = urlNormalizer.normalize(link);
          link = urlNormalizer.extractDomain(link);

          links.add(link);
        } catch (Exception e) {
          System.out.println("Unable to process link: " + link);
        }
      }
    }

    return links;
  }

  private boolean isPageLink(String link) {
    return !link.startsWith("#") && !link.startsWith("mailto") && !link.startsWith("javascript");
  }

}
