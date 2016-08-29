/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc.escape;

import org.apache.ignite.IgniteException;

import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * ODBC escape sequence parse.
 */
public class OdbcEscapeUtils {
    /** Odbc date regexp pattern: '2016-08-23' */
    private static final Pattern DATE_PATTERN = Pattern.compile("^'\\d{4}-\\d{2}-\\d{2}'$");

    /** Odbc time regexp pattern: '14:33:44' */
    private static final Pattern TIME_PATTERN = Pattern.compile("^'\\d{2}:\\d{2}:\\d{2}'$");

    /** Odbc timestamp regexp pattern: '2016-08-23 14:33:44.12345' */
    private static final Pattern TIMESTAMP_PATTERN =
        Pattern.compile("^'\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d+)?'$");

    /** GUID regexp pattern: '12345678-9abc-def0-1234-123456789abc' */
    private static final Pattern GUID_PATTERN =
        Pattern.compile("^'\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}'$");

    /**
     * Parse escape sequence.
     *
     * @param text Original text.
     * @return Result.
     */
    public static String parse(String text) {
        if (text == null)
            throw new IgniteException("Text cannot be null.");

        return parse0(text.trim(), 0, false).result();
    }

    /**
     * Internal parse routine.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param earlyExit When set to {@code true} we must return as soon as single expression is parsed.
     * @return Parse result.
     */
    private static OdbcEscapeParseResult parse0(String text, int startPos, boolean earlyExit) {
        StringBuilder res = new StringBuilder();

        int curPos = startPos;

        int plainPos = startPos;
        int openPos = -1;

        LinkedList<OdbcEscapeParseResult> nested = null;

        while (curPos < text.length()) {
            char curChar = text.charAt(curPos);

            if (curChar == '{') {
                if (openPos == -1) {
                    // Top-level opening brace. Append previous portion and remember current position.
                    res.append(text, plainPos, curPos);

                    openPos = curPos;
                }
                else {
                    // Nested opening brace -> perform recursion.
                    OdbcEscapeParseResult nestedRes = parse0(text, curPos, true);

                    if (nested == null)
                        nested = new LinkedList<>();

                    nested.add(nestedRes);

                    curPos += nestedRes.originalLength() - 1;

                    plainPos = curPos + 1;
                }
            }
            else if (curChar == '}') {
                if (openPos == -1)
                    // Close without open -> exception.
                    throw new IgniteException("Malformed escape sequence " +
                        "(closing curly brace without opening curly brace): " + text);
                else {
                    String parseRes;

                    if (nested == null)
                        // Found sequence without nesting, process it.
                        parseRes = parseExpression(text, openPos, curPos + 1 - openPos);
                    else {
                        // Special case to process nesting.
                        String res0 = appendNested(text, openPos, curPos + 1, nested);

                        nested = null;

                        parseRes = parseExpression(res0, 0, res0.length());
                    }

                    if (earlyExit)
                        return new OdbcEscapeParseResult(startPos, curPos + 1 - startPos, parseRes);
                    else
                        res.append(parseRes);

                    openPos = -1;

                    plainPos = curPos + 1;
                }
            }

            curPos++;
        }

        if (openPos != -1)
            throw new IgniteException("Malformed escape sequence (closing curly brace missing): " + text);

        if (curPos > plainPos)
            res.append(text, plainPos, curPos);

        return new OdbcEscapeParseResult(startPos, curPos - startPos + 1, res.toString());
    }

    /**
     * Parse concrete expression.
     *
     * @param text Text.
     * @param startPos Start position within text.
     * @param len Length.
     * @return Result.
     */
    private static String parseExpression(String text, int startPos, int len) {
        assert validSubstring(text, startPos, len);

        char firstChar = text.charAt(startPos);

        if (firstChar == '{') {
            char lastChar = text.charAt(startPos + len - 1);

            if (lastChar != '}')
                throw new IgniteException("Failed to parse escape sequence because it is not enclosed: " +
                    substring(text, startPos, len));

            OdbcEscapeToken token = parseToken(text, startPos, len);

//            if (token.type().standard())
            return parseStandardExpression(text, startPos, len, token);
           /* else
                throw new IgniteException("Unsupported escape sequence token [text=" +
                    substring(text, startPos, len) + ", token=" + token.type().body() + ']');*/
        }
        else {
            // Nothing to escape, return original string.
            if (startPos == 0 || text.length() == len)
                return text;
            else
                return substring(text, startPos, len);
        }
    }

    /**
     * Get escape sequence info.
     *
     * @param text Text.
     * @param startPos Start position.
     * @return Escape sequence info.
     */
    private static OdbcEscapeToken parseToken(String text, int startPos, int len) {
        assert validSubstring(text, startPos, len);
        assert text.charAt(startPos) == '{';

        int pos = startPos + 1;

        while (Character.isWhitespace(text.charAt(pos)))
            pos++;

        OdbcEscapeType curTyp = null;
        boolean empty = false;

        for (OdbcEscapeType typ : OdbcEscapeType.sortedValues()) {
            if (text.startsWith(typ.body(), pos)) {
                if (typ != OdbcEscapeType.ESCAPE_WO_TOKEN) {
                    pos += typ.body().length();

                    empty = (startPos + len == pos + 1);

                    if (!empty && typ.standard()) {
                        char charAfter = text.charAt(pos);

                        if (!Character.isWhitespace(charAfter))
                            throw new IgniteException("Unexpected escape sequence token: " +
                                substring(text, startPos, len));
                    }
                }

                curTyp = typ;

                break;
            }
        }

        if (curTyp == null)
            throw new IgniteException("Unsupported escape sequence: " + substring(text, startPos, len));

        if (empty && !curTyp.allowEmpty())
            throw new IgniteException("Escape sequence cannot be empty: " + substring(text, startPos, len));

        return new OdbcEscapeToken(curTyp, pos - (startPos + 1));
    }

    /**
     * Parse standard token.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param len Length.
     * @param token Token.
     * @return Result.
     */
    private static String parseStandardExpression(String text, int startPos, int len, OdbcEscapeToken token) {
        assert validSubstring(text, startPos, len);

        // Get expression borders.
        int startPos0 = startPos + 1 /* open brace */ + token.length() /* token. */;
        int len0 = len - 1 /* open brace */ - token.length() /* token */ - 1 /* close brace */;

        switch (token.type()) {
            case SCALAR_FUNCTION:
                return parseScalarExpression(text, startPos0, len0);

            case GUID:
                return parseExpression(text, startPos0, len0, token.type(), GUID_PATTERN);

            case DATE:
                return parseExpression(text, startPos0, len0, token.type(), DATE_PATTERN);

            case TIME:
                return parseExpression(text, startPos0, len0, token.type(), TIME_PATTERN);

            case TIMESTAMP:
                return parseExpression(text, startPos0, len0, token.type(), TIMESTAMP_PATTERN);

            case ESCAPE:
            case ESCAPE_WO_TOKEN:
                return parseLikeEscCharacterExpression(text, startPos0, len0);

            default:
                throw new IgniteException("Unsupported escape sequence token [text=" +
                    substring(text, startPos, len) + ", token=" + token.type().body() + ']');
        }
    }

    /**
     * Parse scalar function expression.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param len Length.
     * @return Parsed expression.
     */
    private static String parseScalarExpression(String text, int startPos, int len) {
        return substring(text, startPos, len).trim();
    }

    /**
     * Parse LIKE escape character expression.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param len Length.
     * @return Parsed expression.
     */
    private static String parseLikeEscCharacterExpression(String text, int startPos, int len) {
        return "ESCAPE " + substring(text, startPos, len).trim();
    }

    /**
     * Parse concrete expression.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param len Length.
     * @return Parsed expression.
     */
    private static String parseExpression(String text, int startPos, int len, OdbcEscapeType type, Pattern pattern) {
        String val = substring(text, startPos, len).trim();

        if (!pattern.matcher(val).matches())
            throw new IgniteException("Invalid " + type + " escape sequence: " + substring(text, startPos, len));

        return val;
    }

    /**
     * Append nested results.
     *
     * @param text Original text.
     * @param startPos Start position.
     * @param endPos End position.
     * @param nestedRess Nested results.
     * @return Result.
     */
    private static String appendNested(String text, int startPos, int endPos,
        LinkedList<OdbcEscapeParseResult> nestedRess) {
        StringBuilder res = new StringBuilder();

        int curPos = startPos;

        for (OdbcEscapeParseResult nestedRes : nestedRess) {
            // Append text between current position and replace.
            res.append(text, curPos, nestedRes.originalStart());

            // Append replaced text.
            res.append(nestedRes.result());

            // Advance position.
            curPos = nestedRes.originalStart() + nestedRes.originalLength();
        }

        // Append remainder.
        res.append(text, curPos, endPos);

        return res.toString();
    }

    /**
     * Perform "substring" using start position and length.
     *
     * @param text Text.
     * @param startPos Start position.
     * @param len Length.
     * @return Substring.
     */
    private static String substring(String text, int startPos, int len) {
        assert validSubstring(text, startPos, len);

        return text.substring(startPos, startPos + len);
    }

    /**
     * Check whether substring is valid.
     *
     * @param text Substring.
     * @param startPos Start position.
     * @param len Length.
     * @return {@code True} if valid.
     */
    private static boolean validSubstring(String text, int startPos, int len) {
        return text != null && startPos + len <= text.length();
    }

    /**
     * Private constructor.
     */
    private OdbcEscapeUtils() {
        // No-op.
    }
}
