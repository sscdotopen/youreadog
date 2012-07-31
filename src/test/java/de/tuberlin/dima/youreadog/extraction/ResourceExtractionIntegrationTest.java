package de.tuberlin.dima.youreadog.extraction;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.tuberlin.dima.youreadog.hadoop.writables.Resource;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertTrue;

public class ResourceExtractionIntegrationTest {

  @Test
  public void spiegelDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));

    assertViewersFound(resources, "spiegel.ivwbox.de", "adserv.quality-channel.de", "facebook.com");
  }

  //@Test
  public void zalandoDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    assertViewersFound(resources, "everestjs.net", "pixel.everesttech.net", "fls.doubleclick.net", "uidbox.uimserv.net",
        "sonar.sociomantic.com", "googleadservices.com", "google-analytics.com", "connect.facebook.net");
  }

  @Test
  public void techcrunchCom() throws IOException {

    Iterable<Resource> resources = extractResources("http://techcrunch.com",
        Resources.getResource("techcrunch.com.html"));

//    for (Resource resource : resources) {
//      System.out.println(resource.type() + " " + resource.url());
//    }

    //TODO add viewers to check
  }

  private void assertViewersFound(Iterable<Resource> resources, String... urls) {
    for (String url : urls) {
      boolean found = false;
      for (Resource resource : resources) {
        if (resource.url().contains(url)) {
          found = true;
          break;
        }
      }
      assertTrue("Resource extraction missed [" + url + "]", found);
    }
  }

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}