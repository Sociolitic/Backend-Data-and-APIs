import pytumblr
import pandas as pd
def tumblrsearch(search):
    client = pytumblr.TumblrRestClient(
      'KD3YxdO7uigsFn9v8iWD8KHEJIhlJpj4WUfDYXLX0KulX540Bg',
      'oACAMUWjAilXiIglpGh2ibZLJwwGrqRu4y9dpNJE28VjR4FLgu',
      'prHg38NXZQigKYFgxxKxLGO0ePkwksqLkjF9NI653ZbIKB27tx',
      'pdwCtHPdCZO1J5MxSkoWr4lunFjGx4gxbcUn4Y5Axi2b1BJScN'
    )
    output=client.tagged(tag=search,filter="text")
    blog_dict = {"blog_name":[],"id":[],"post_url":[],"timestamp":[],"tags":[],"title":[],"body":[],"summary":[]}
    for blog in output:
        if "caption" not in blog:
            blog_dict["title"].append(blog["title"])
            if "body" in blog:
                blog_dict["body"].append(blog["body"])
            else:
                blog_dict["body"].append("")

        else:
            blog_dict['title'].append("image")
            blog_dict["body"].append(blog["caption"])
        blog_dict["summary"].append(blog["summary"])
        blog_dict["blog_name"].append(blog["blog_name"])
        blog_dict["id"].append(blog["id"])
        blog_dict["post_url"].append(blog["post_url"])
        blog_dict["timestamp"].append(blog["timestamp"])
        blog_dict["tags"].append(blog["tags"])
    topics_data = pd.DataFrame(blog_dict)
    json = topics_data.to_json(orient="records")
    return json
