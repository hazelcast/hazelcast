---
---

var index = lunr(function () {
    this.field('body');
    this.ref('url');
});

var documentTitles = {};

<% for file in @manifest.files: %>
<% for section in file.sections: %>
documentTitles["<%= @baseUrl + file.slug %>.html#<%= section.slug %>"] = "<%= section.title.replace(/"/g, '\\"') %>";
index.add({
    url: "<%= @baseUrl + file.slug %>.html#<%= section.slug %>",
    title: "<%= section.title.replace(/"/g, '\\"') %>",
    body: "<%= section.markdown.replace(/\n|\r/g, " ").replace(/"/g, '\\"') %>"
});
<% end %>
<% end %>
