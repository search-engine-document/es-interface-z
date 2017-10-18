package com.cobub.es.servlet;

import com.cobub.es.utils.StringUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by feng.wei on 2015/12/16.
 */
public abstract class BaseServlet extends HttpServlet {

    @Override
    public void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String reqMethod = req.getParameter("method");
        try {
            if (!StringUtil.isEmpty(reqMethod)) {
                Method method = this.getClass().getMethod(reqMethod, HttpServletRequest.class, HttpServletResponse.class);
                method.invoke(this,req,resp);
            }

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }


}
