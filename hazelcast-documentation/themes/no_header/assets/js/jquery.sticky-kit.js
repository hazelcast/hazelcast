// Generated by CoffeeScript 1.6.3
/**
@license Sticky-kit v1.0.1 | WTFPL | Leaf Corcoran 2013 | http://leafo.net
*/


(function() {
  var $, win;

  $ = this.jQuery;

  win = $(window);

  $.fn.stick_in_parent = function(opts) {
    var elm, inner_scrolling, offset_top, parent_selector, sticky_class, _fn, _i, _len;
    if (opts == null) {
      opts = {};
    }
    sticky_class = opts.sticky_class, inner_scrolling = opts.inner_scrolling, parent_selector = opts.parent, offset_top = opts.offset_top;
    if (offset_top == null) {
      offset_top = 0;
    }
    if (parent_selector == null) {
      parent_selector = void 0;
    }
    if (inner_scrolling == null) {
      inner_scrolling = true;
    }
    if (sticky_class == null) {
      sticky_class = "is_stuck";
    }
    _fn = function(elm, padding_bottom, parent_top, parent_height, top, height) {
      var bottomed, fixed, float, last_pos, offset, parent, recalc, reset_width, spacer, tick;
      parent = elm.parent();
      if (parent_selector != null) {
        parent = parent.closest(parent_selector);
      }
      if (!parent.length) {
        throw "failed to find stick parent";
      }
      recalc = function() {
        var border_top, padding_top, sizing_elm;
        border_top = parseInt(parent.css("border-top-width"), 10);
        padding_top = parseInt(parent.css("padding-top"), 10);
        padding_bottom = parseInt(parent.css("padding-bottom"), 10);
        parent_top = parent.offset().top + border_top + padding_top;
        parent_height = parent.height();
        sizing_elm = elm.is(".is_stuck") ? spacer : elm;
        top = sizing_elm.offset().top - parseInt(sizing_elm.css("margin-top"), 10) - offset_top;
        return height = sizing_elm.outerHeight(true);
      };
      recalc();
      if (height === parent_height) {
        return;
      }
      float = elm.css("float");
      spacer = $("<div />").css({
        width: elm.outerWidth(true),
        height: height,
        display: elm.css("display"),
        "vertical-align": elm.css("vertical-align"),
        float: float
      });
      fixed = false;
      bottomed = false;
      last_pos = void 0;
      offset = offset_top;
      reset_width = false;
      tick = function() {
        var css, delta, scroll, will_bottom, win_height;
        scroll = win.scrollTop();
        if (last_pos != null) {
          delta = scroll - last_pos;
        }
        last_pos = scroll;
        if (fixed) {
          will_bottom = scroll + height + offset > parent_height + parent_top;
          if (bottomed && !will_bottom) {
            bottomed = false;
            elm.css({
              position: "fixed",
              bottom: "",
              top: offset
            }).trigger("sticky_kit:unbottom");
          }
          if (scroll < top) {
            fixed = false;
            offset = offset_top;
            if (float === "left" || float === "right") {
              elm.insertAfter(spacer);
            }
            spacer.detach();
            css = {
              position: "",
              top: ""
            };
            if (reset_width) {
              css.width = "";
            }
            elm.css(css).removeClass(sticky_class).trigger("sticky_kit:unstick");
          }
          if (inner_scrolling) {
            win_height = win.height();
            if (height > win_height) {
              if (!bottomed) {
                offset -= delta;
                offset = Math.max(win_height - height, offset);
                offset = Math.min(offset_top, offset);
                if (fixed) {
                  elm.css({
                    top: offset + "px"
                  });
                }
              }
            }
          }
        } else {
          if (scroll > top) {
            fixed = true;
            css = {
              position: "fixed",
              top: offset
            };
            if (float === "none" && elm.css("display") === "block") {
              css.width = elm.width() + "px";
              reset_width = true;
            }
            elm.css(css).addClass(sticky_class).after(spacer);
            if (float === "left" || float === "right") {
              spacer.append(elm);
            }
            elm.trigger("sticky_kit:stick");
          }
        }
        if (fixed) {
          if (will_bottom == null) {
            will_bottom = scroll + height + offset > parent_height + parent_top;
          }
          if (!bottomed && will_bottom) {
            bottomed = true;
            if (parent.css("position") === "static") {
              parent.css({
                position: "relative"
              });
            }
            return elm.css({
              bottom: padding_bottom,
              top: ""
            }).trigger("sticky_kit:bottom");
          }
        }
      };
      win.on("scroll", tick);
      setTimeout(tick, 0);
      return $(document.body).on("sticky_kit:recalc", function() {
        recalc();
        return tick();
      });
    };
    for (_i = 0, _len = this.length; _i < _len; _i++) {
      elm = this[_i];
      _fn($(elm));
    }
    return this;
  };

}).call(this);
