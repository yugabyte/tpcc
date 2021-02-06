package com.oltpbenchmark.util;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

/**
 * Utility class for printing boxes around text
 * @author pavlo
 */
public abstract class StringBoxUtil {

    private static final Pattern LINE_SPLIT = Pattern.compile("\n");

    public static final String[] UNICODE_HEAVYBOX_CORNERS = {"\u250F", "\u2513", "\u2517", "\u251B"};
    public static final String UNICODE_HEAVYBOX_VERTICAL = "\u2503";
    public static final String UNICODE_HEAVYBOX_HORIZONTAL = "\u2501";

    public static String box(String str, String horzMark, String vertMark, Integer max_len, String[] corners) {
        String[] lines = LINE_SPLIT.split(str);
        if (lines.length == 0)
            return ("");
    
        // CORNERS: 
        //  0: Top-Left
        //  1: Top-Right
        //  2: Bottom-Left
        //  3: Bottom-Right
        if (corners == null) {
            corners = new String[]{horzMark, horzMark, horzMark, horzMark};
        }
        
        if (max_len == null) {
            for (String line : lines) {
                if (max_len == null || line.length() > max_len)
                    max_len = line.length();
            } // FOR
        }
    
        final String top_line = corners[0] + StringUtils.repeat(horzMark, max_len + 2) + corners[1]; // padding - two corners
        final String bot_line = corners[2] + StringUtils.repeat(horzMark, max_len + 2) + corners[3]; // padding - two corners
        final String f = "%s %-" + max_len + "s %s\n";
    
        StringBuilder sb = new StringBuilder();
        sb.append(top_line).append("\n");
        for (String line : lines) {
            sb.append(String.format(f, vertMark, line, vertMark));
        } // FOR
        sb.append(bot_line);
    
        return (sb.toString());
    }
    
    /**
     * Heavy unicode border box
     */
    public static String heavyBox(String str) {
        return box(str, StringBoxUtil.UNICODE_HEAVYBOX_HORIZONTAL,
                        StringBoxUtil.UNICODE_HEAVYBOX_VERTICAL,
                        null,
                        StringBoxUtil.UNICODE_HEAVYBOX_CORNERS);
    }


}
