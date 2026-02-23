"use client"

import { useState, useEffect } from "react"

/**
 * Detects whether the mobile keyboard is open.
 * With overlays-content viewport, visualViewport doesn't resize,
 * so we detect via focus/blur on input/textarea elements
 * and use navigator.virtualKeyboard or a fallback estimate for height.
 *
 * Returns { isKeyboardOpen, keyboardHeight }
 */
export function useKeyboardOpen() {
    const [isKeyboardOpen, setIsKeyboardOpen] = useState(false)
    const [keyboardHeight, setKeyboardHeight] = useState(0)

    useEffect(() => {
        // Only on mobile
        if (typeof window === 'undefined') return
        const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
        if (!isMobile) return

        // Try to enable VirtualKeyboard API
        const vk = (navigator as any).virtualKeyboard
        if (vk) {
            vk.overlaysContent = true
        }

        const handleFocusIn = (e: FocusEvent) => {
            const target = e.target as HTMLElement
            if (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA') {
                setIsKeyboardOpen(true)
                // Estimate keyboard height — use VirtualKeyboard API if available
                if (vk?.boundingRect?.height) {
                    setKeyboardHeight(vk.boundingRect.height)
                } else {
                    // Fallback: estimate ~40% of screen for keyboard
                    setKeyboardHeight(Math.round(window.innerHeight * 0.4))
                }
            }
        }

        const handleFocusOut = (e: FocusEvent) => {
            const related = e.relatedTarget as HTMLElement | null
            // Only close if not focusing another input
            if (!related || (related.tagName !== 'INPUT' && related.tagName !== 'TEXTAREA')) {
                setIsKeyboardOpen(false)
                setKeyboardHeight(0)
            }
        }

        // Also listen to VirtualKeyboard geometry changes
        const handleGeometryChange = () => {
            if (vk?.boundingRect) {
                const h = vk.boundingRect.height
                setKeyboardHeight(h)
                setIsKeyboardOpen(h > 0)
            }
        }

        document.addEventListener('focusin', handleFocusIn)
        document.addEventListener('focusout', handleFocusOut)
        if (vk) {
            vk.addEventListener('geometrychange', handleGeometryChange)
        }

        return () => {
            document.removeEventListener('focusin', handleFocusIn)
            document.removeEventListener('focusout', handleFocusOut)
            if (vk) {
                vk.removeEventListener('geometrychange', handleGeometryChange)
            }
        }
    }, [])

    return { isKeyboardOpen, keyboardHeight }
}
