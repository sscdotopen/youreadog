package de.tuberlin.dima.youreadog.extraction;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;

public class ResourceExtractor {
	
  private final Pattern originPattern = Pattern.compile(
		  "(<)(iframe|script|link|img)(.+)(</iframe>|</script>|/>)");
  
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
  
  private final Pattern javascriptPattern = Pattern.compile(
		  "(http://|https://|ftp://)((\\w|[^a-zA-Z_0-9/])+)(\")((\\w|[^a-zA-Z_0-9/])+)([\\.]{1})(com|org|net|gov|mil|biz" +
		  "|info|mobi|name|aero|jobs|museum|travel|[a-z]{2})");

  private final URLNormalizer urlNormalizer = new URLNormalizer();

  public Set<RessourceObject> extractScripts(String sourceUrl, String html) {

    if (sourceUrl == null) {
      return Collections.emptySet();
    }

    String prefixForInternalLinks = urlNormalizer.createPrefixForInternalLinks(sourceUrl);

    Set<RessourceObject> resources = Sets.newHashSet();

    Matcher matcher = originPattern.matcher(html);
    boolean findSomething;
    while (matcher.find()) {
      String type = matcher.group(2).toLowerCase();
      Matcher matcher2 = resourcePattern.matcher(matcher.group(3));
      findSomething = false;
      // check ressouces for images, scripts, iframes and links
      while (matcher2.find()) {
    	  findSomething = true;
	      String resource = matcher2.group();
	      resource = urlNormalizer.expandIfInternalLink(prefixForInternalLinks, resource);
	      // normalize link
	      try {
	        resource = urlNormalizer.normalize(resource);
	        resource = urlNormalizer.extractDomain(resource);
	      } catch (MalformedURLException e) {
	        System.out.println("Unable to process resource: " + resource);
	      }
	      resources.add(new RessourceObject(resource,type));
      }
      // check inner javascript ressources
      if(type.equals("script") && !findSomething) {
    	  Matcher matcher3 = javascriptPattern.matcher(matcher.group(3));
    	  while (matcher3.find()) {
    		  // concatenate ressource string
    	      String resource = matcher3.group(1).concat(matcher3.group(5)).concat(matcher3.group(7)).concat(matcher3.group(8));
    	      // normalize link
    	      try {
    	        resource = urlNormalizer.normalize(resource);
    	        resource = urlNormalizer.extractDomain(resource);
    	      } catch (MalformedURLException e) {
    	        System.out.println("Unable to process resource: " + resource);
    	      }
    	      resources.add(new RessourceObject(resource,type));
    	  }
      }
    }
    return resources;
  }

}
