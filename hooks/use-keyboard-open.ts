"use client"

import { useState, useEffect } from "react"

/**
 * Detects whether the mobile keyboard is open by comparing
 * window.visualViewport.height to window.innerHeight.
 * Returns true when keyboard takes up significant screen space.
 */
export function useKeyboardOpen() {
    const [isKeyboardOpen, setIsKeyboardOpen] = useState(false)

    useEffect(() => {
        const vv = window.visualViewport
        if (!vv) return // Not supported (desktop)

        const threshold = 150 // px — keyboard is at least 150px tall

        const handleResize = () => {
            const keyboardHeight = window.innerHeight - vv.height
            setIsKeyboardOpen(keyboardHeight > threshold)
        }

        vv.addEventListener("resize", handleResize)
        vv.addEventListener("scroll", handleResize)

        return () => {
            vv.removeEventListener("resize", handleResize)
            vv.removeEventListener("scroll", handleResize)
        }
    }, [])

    return isKeyboardOpen
}
