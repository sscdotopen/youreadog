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

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class LinkExtractionTest {

  private LinkExtractor extractor;

  @Before
  public void initializeExtractor () {
    extractor = new LinkExtractor();
  }

  @Test
  public void testHrefExtraction() {

    String html = "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    Set<String> links = extractor.extractLinks("zalando.de", html);
    assertEquals(1, links.size());
    assertEquals("www.zalando.de", new LinkedList<String>(links).get(0));
  }

  @Test
  public void testEncodedHrefExtraction() {
    String html = "<a name=\"header.logo\" href=\"http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    Set<String> links = extractor.extractLinks("zalando.de", html);
    assertEquals(1, links.size());
    assertEquals("g.live.com", new LinkedList<String>(links).get(0));
  }

  @Test
  public void testSeveralExtractions () {

    String html = "<a name=\"header.logo\" href=\"http&#58;&#47;&#47;g.live.com&#47;9uxp9en-us&#47;\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    html += "<a name=\"header.logo\" href=\"http://www.zalando.de/index.html\"><img height=\"53\" width=\"190\" src=\"http://skin.ztat.net/s/04g/img/logos/shops/schuhe.jpg\" alt=\"Schuhe und Mode bei Zalando.de\" title=\"Schuhe und Mode bei Zalando.de\"/></a>";
    html += "test<a href=\"https:/www.test.de\">test</a>";
    Set<String> links = extractor.extractLinks("zalando.de", html);
    assertEquals(3, links.size());
  }

}
