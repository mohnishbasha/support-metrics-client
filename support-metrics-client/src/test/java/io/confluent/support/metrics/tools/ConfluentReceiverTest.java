/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.support.metrics.tools;


import org.junit.Test;
import java.util.Calendar;
import static org.assertj.core.api.Assertions.assertThat;


public class ConfluentReceiverTest {

  @Test
  public void testGetDateValid() {
    String key = "c0/127.0.1.1/2015/10/02/restDoesNotMatter";
    Calendar date = ConfluentReceiver.getDate(key);
    assertThat(date.get(Calendar.YEAR)).isEqualTo(2015);
    assertThat(date.get(Calendar.MONTH) + 1).isEqualTo(10);
    assertThat(date.get(Calendar.DAY_OF_MONTH)).isEqualTo(2);
  }

  @Test
  public void testGetDateInvalid() {
    String[] invalidKeys = {"c0/127.0.1.1/10/02/restDoesNotMatter", "10/02/", "c0/127.0.1.1//10/02/restDoesNotMatter", null, ""};
    for (String key : invalidKeys) {
      Calendar date = ConfluentReceiver.getDate(key);
      assertThat(date).isNull();
    }
  }

  @Test
  public void testGetHostNameValid() {
    String key = "c0/127.0.1.1/restDoesNotMatter";
    String hostName = ConfluentReceiver.getHostname(key);
    assertThat(hostName).isEqualTo("127.0.1.1");
  }

  @Test
  public void testGetHostNameInvalid() {
    String[] invalidKeys = {"c0/127.0.1./restDoesNotMatter", "c0/2015/", "c0", "c0//2015", "", null};
    for (String key: invalidKeys) {
      String hostName = ConfluentReceiver.getHostname(key);
      assertThat(hostName).isEmpty();
    }
  }

  @Test
  public void testGetCustomerIdValid() {
    String key = "c0/restDoesNotMatter";
    String customerId = ConfluentReceiver.getCustomerID(key);
    assertThat(customerId).isEqualTo("c0");
  }

  @Test
  public void testGetCustomerIdInvalid() {
    String[] invalidKeys = {"0c0/restDoesNotMatter", "/c0/2015/", "/c0", "//2015", "", null};
    for (String key: invalidKeys) {
      String customerId = ConfluentReceiver.getCustomerID(key);
      assertThat(customerId).isEmpty();
    }
  }

}
