/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;

public class NameNodeConfServlet extends HttpServlet {

  NameNode nn = null;
  Configuration nnConf = null;
  private static long lastId = 0; // Used to generate unique element IDs

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext context = this.getServletContext();
    nnConf = (Configuration) context.getAttribute("name.conf");
    nn = (NameNode) context.getAttribute("name.node");
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    if (req.getParameter("setPersistBlocks") != null) {
      this.nn.getNamesystem().setPersistBlocks(
          req.getParameter("setPersistBlocks").equals("ON") ? true : false);
      resp.sendRedirect("/nnconf");
      return;
    }
    if (req.getParameter("setPermissionAuditLog") != null) {
      this.nn.getNamesystem().setPermissionAuditLog(
          req.getParameter("setPermissionAuditLog").equals("ON") ? true : false);
      resp.sendRedirect("/nnconf");
      return;
    }
    PrintWriter out = resp.getWriter();
    String hostname = this.nn.getNameNodeAddress().toString();
    out.print("<html><head>");
    out.printf("<title>%s NameNode Admininstration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" "
        + "href=\"/static/hadoop.css\">\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/dfshealth.jsp\">%s</a> "
        + "NameNode Configuration Admin</h1>\n", hostname);
    showOptions(out);
    out.print("</body></html>\n");
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp);
  }

  private void showOptions(PrintWriter out) {
    out.print("<h2>Persist blocks to edits log on allocation</h2>\n");
    out.printf("<p>Persisting Blocks:%s",
        generateSelect(Arrays.asList("ON,OFF".split(",")), this.nn
            .getNamesystem().getPersistBlocks() ? "ON" : "OFF",
            "/nnconf?setPersistBlocks=<CHOICE>"));
    out.print("<h2>Enable permission audit log</h2>/n");
    out.printf("<p>Permission Audit Log Blocks:%s",
        generateSelect(Arrays.asList("ON,OFF".split(",")), this.nn
            .getNamesystem().getPermissionAuditLog() ? "ON" : "OFF",
            "/nnconf?setPermissionAuditLog=<CHOICE>"));
  }

  /**
   * Generate a HTML select control with a given list of choices and a given
   * option selected. When the selection is changed, take the user to the
   * <code>submitUrl</code>. The <code>submitUrl</code> can be made to include
   * the option selected -- the first occurrence of the substring
   * <code>&lt;CHOICE&gt;</code> will be replaced by the option chosen.
   */
  private String generateSelect(Iterable<String> choices,
      String selectedChoice, String submitUrl) {
    StringBuilder html = new StringBuilder();
    String id = "select" + lastId++;
    html.append("<select id=\"" + id + "\" name=\"" + id + "\" "
        + "onchange=\"window.location = '" + submitUrl
        + "'.replace('<CHOICE>', document.getElementById('" + id
        + "').value);\">\n");
    for (String choice : choices) {
      html.append(String.format("<option value=\"%s\"%s>%s</option>\n", choice,
          (choice.equals(selectedChoice) ? " selected" : ""), choice));
    }
    html.append("</select>\n");
    return html.toString();
  }

}
