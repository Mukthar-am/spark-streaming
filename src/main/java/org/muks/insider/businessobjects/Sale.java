package org.muks.insider.businessobjects;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;

public class Sale implements Serializable {

    private String user_id;
    private BigDecimal cart_amount;
    private String product_category;
    private String id = "UNDEF";
    private String imgurl = "UNDEF";
    private String name;
    private BigDecimal product_price;
    private String url = "UNDEF";

    public Sale() { }

    public Sale(String user_id,
                BigDecimal cart_amount,
                String product_category,
                String id,
                String imgurl,
                String name,
                BigDecimal product_price,
                String url) {

        this.user_id = user_id;
        this.cart_amount = cart_amount;
        this.product_category = product_category;
        this.id = id;
        this.imgurl = imgurl;
        this.name = name;
        this.product_price = product_price;
        this.url = url;
    }

    public String getuser_id() { return user_id; }
    public void setuser_id(String id) { this.user_id = id; }


    public BigDecimal getCart_amount() {
        return this.cart_amount;
    }

    public void setCart_amount(BigDecimal amount) { this.cart_amount = amount; }

    public String getProduct_category() {
        return this.product_category;
    }
    public void setProduct_category(String product_category) { this.product_category = product_category; }

    public String getId() {
        return this.id;
    }
    public void setId(String id) { this.id = id; }

    public String getimgurl()  { return this.imgurl; }
    public void setimgurl(String imgUrl) { this.imgurl = imgUrl; }


    public BigDecimal getproduct_price() { return product_price; }
    public void setproduct_price(BigDecimal price) { this.product_price = price; }

    public String geturl() { return this.url; }
    public void seturl() { this.url = url; }

    public String getname() { return this.name; }
    public void setname(String name) { this.name = name; }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{}, {}, {}, {}, {}, {}", user_id, id, product_category, imgurl, name, url, cart_amount, product_price);
    }
}
