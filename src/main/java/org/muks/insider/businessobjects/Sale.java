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
    private BigDecimal price;
    private String url = "UNDEF";

    public Sale() { }

    public Sale(String user_id,
                BigDecimal cart_amount,
                String product_category,
                String id,
                String imgUrl,
                String name,
                BigDecimal price,
                String url) {

        this.user_id = user_id;
        this.cart_amount = cart_amount;
        this.product_category = product_category;
        this.id = id;
        this.imgurl = imgUrl;
        this.name = name;
        this.price = price;
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

    public String getImgUrl()  { return this.imgurl; }
    public void setImgUrl(String imgUrl) { this.imgurl = imgUrl; }


    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }

    public String getUrl() { return this.url; }
    public void setUrl() { this.url = url; }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Sale'{'cart_amount={0}, " +
                        "product_category={1}, " +
                        "id={2}'}," +
                        "imgUrl={3}," +
                        "name={4}," +
                        "price={5}," +
                        "url={6}'", cart_amount, product_category, id, imgurl, name, price, url);
    }
}