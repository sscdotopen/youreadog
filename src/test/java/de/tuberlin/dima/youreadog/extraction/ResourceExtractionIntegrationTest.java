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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class ResourceExtractionIntegrationTest {

  @Test
  public void spiegelDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://spiegel.de", Resources.getResource("spiegel.de.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }

    //assertViewersFound(resources, "spiegel.ivwbox.de", "adserv.quality-channel.de", "facebook.com");
  }

  @Test
  public void zalandoDe() throws IOException {

    Iterable<Resource> resources = extractResources("http://zalando.de", Resources.getResource("zalando.de.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }

//    assertViewersFound(resources, "everestjs.net", "pixel.everesttech.net", "fls.doubleclick.net", "uidbox.uimserv.net",
//        "sonar.sociomantic.com", "googleadservices.com", "google-analytics.com", "connect.facebook.net");
  }

  @Test
  public void techcrunchCom() throws IOException {

    Iterable<Resource> resources = extractResources("http://techcrunch.com",
        Resources.getResource("techcrunch.com.html"));

    for (Resource resource : resources) {
      System.out.println(resource);
    }
  }


  private void assertViewersFound(Iterable<Resource> resources, String... urls) {
    for (String url : urls) {
      boolean found = false;
      for (Resource resource : resources) {
        if (resource.url().contains(url)) {
          found = true;
          System.out.println("Found resource " + url);
          break;
        }
      }
      //assertTrue("Resource extraction missed [" + url + "]", found);
    }
  }

  Iterable<Resource> extractResources(String sourceUrl, URL page) throws IOException {
    return new ResourceExtractor().extractResources(sourceUrl, Resources.toString(page, Charsets.UTF_8));
  }

}
