package com.cobub.es.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by feng.wei on 2015/12/16.
 */
@WebServlet(name = "/es", urlPatterns = "/es")
public class EsServlet extends BaseServlet {

    public void insert(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("/es has been access");
        resp.getWriter().println("insert");
    }

    public void get(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("/es has been access");
        resp.getWriter().println("get");
    }

}
